//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/timo-kang/fleetrollout/test/utils"
)

const (
	crNS  = "fleet-rollout-e2e"
	monNS = "fleet-monitoring"

	// Two real, pullable images that differ, so a rollout has something to roll.
	imgA = "registry.k8s.io/pause:3.9"
	imgB = "registry.k8s.io/pause:3.10"

	promHealthy     = "http://prom-healthy.fleet-monitoring:9090"
	promUnhealthy   = "http://prom-unhealthy.fleet-monitoring:9090"
	promUnreachable = "http://prom-nonexistent.fleet-monitoring:9090" // no such Service → connection refused

	ownerSelector = "fleetrollout.fleet.fleetrollout.io/owner="
)

// This suite proves the behavior the operator exists for — wave promotion, gate-driven
// rollback, gate-driven pause, and the safety-critical "no data ≠ unhealthy" hold — against
// an in-cluster controller and stub Prometheus servers. (A locally-run controller cannot reach
// ClusterIP Services, so the gate paths are only observable with the controller deployed.)
var _ = Describe("Rollout lifecycle (waves / gate / rollback)", Ordered, func() {
	BeforeAll(func() {
		By("installing CRDs")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")

		By("waiting for the controller-manager to become available")
		Eventually(func(g Gomega) {
			out, err := utils.Run(exec.Command("kubectl", "rollout", "status",
				"deploy/fleetrollout-controller-manager", "-n", "fleetrollout-system", "--timeout=20s"))
			g.Expect(err).NotTo(HaveOccurred(), out)
		}, 3*time.Minute, 5*time.Second).Should(Succeed())

		By("creating the test namespaces")
		for _, ns := range []string{crNS, monNS} {
			_, _ = utils.Run(exec.Command("kubectl", "create", "ns", ns))
		}

		By("deploying the stub Prometheus servers")
		_, err = utils.Run(exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/prometheus-stub.yaml"))
		Expect(err).NotTo(HaveOccurred(), "Failed to apply stub Prometheus")
		for _, d := range []string{"prom-healthy", "prom-unhealthy"} {
			_, err = utils.Run(exec.Command("kubectl", "-n", monNS, "rollout", "status", "deploy/"+d, "--timeout=120s"))
			Expect(err).NotTo(HaveOccurred(), "stub %s not ready", d)
		}
	})

	AfterAll(func() {
		By("tearing down the lifecycle test resources")
		_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", crNS, monNS, "--ignore-not-found", "--wait=false"))
		_, _ = utils.Run(exec.Command("make", "undeploy"))
		_, _ = utils.Run(exec.Command("make", "uninstall"))
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			out, _ := utils.Run(exec.Command("kubectl", "get", "fleetrollout", "-n", crNS, "-o", "yaml"))
			_, _ = fmt.Fprintf(GinkgoWriter, "FleetRollouts:\n%s\n", out)
			out, _ = utils.Run(exec.Command("kubectl", "logs", "-l", "control-plane=controller-manager",
				"-n", "fleetrollout-system", "--tail=80"))
			_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n%s\n", out)
		}
	})

	It("S1: promotes wave-by-wave to Done while the gate is healthy", func() {
		const name = "s1-healthy"
		applyRollout(name, imgA, "OnFailure", promHealthy, 60)
		waitPhase(name, "Done", 3*time.Minute)

		By("upgrading to a new image — every inter-wave gate passes, so it reaches Done")
		upgradeImage(name, imgB)
		waitPhase(name, "Done", 3*time.Minute)
		Expect(frField(name, "{.status.lastGoodImage}")).To(Equal(imgB))
		Expect(frField(name, "{.status.observedGeneration}")).To(Equal("2"))
	})

	It("S2: auto-rolls-back to last-good when the gate fails (OnFailure)", func() {
		const name = "s2-rollback"
		applyRollout(name, imgA, "OnFailure", "", 0) // no gate → Done, records lastGood=imgA
		waitPhase(name, "Done", 3*time.Minute)

		By("upgrading behind an unhealthy gate — the wave fails and rolls back")
		upgradeWithGate(name, imgB, promUnhealthy, 15)
		waitPhase(name, "RolledBack", 4*time.Minute)

		By("every pod is back on the last-good image")
		Eventually(func(g Gomega) {
			g.Expect(podImages(name)).To(Equal([]string{imgA}))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	It("S3: HOLDS and never rolls back when Prometheus is unreachable (C1 safety)", func() {
		const name = "s3-unreachable"
		applyRollout(name, imgA, "OnFailure", "", 0)
		waitPhase(name, "Done", 3*time.Minute)

		By("upgrading behind an UNREACHABLE gate — missing data must not be treated as unhealthy")
		upgradeWithGate(name, imgB, promUnreachable, 15)
		Eventually(func(g Gomega) {
			g.Expect(condReason(name, "HealthGatePassed")).To(Equal("MonitoringUnavailable"))
		}, 2*time.Minute, 5*time.Second).Should(Succeed())

		By("and it never rolls back, even well past the gate timeout")
		Consistently(func(g Gomega) {
			g.Expect(frField(name, "{.status.phase}")).NotTo(Equal("RolledBack"))
		}, 45*time.Second, 5*time.Second).Should(Succeed())
	})

	It("S4: pauses (no rollback) when the gate fails and rollbackPolicy=Never", func() {
		const name = "s4-paused"
		applyRollout(name, imgA, "Never", "", 0)
		waitPhase(name, "Done", 3*time.Minute)

		By("upgrading behind an unhealthy gate with rollbackPolicy=Never")
		upgradeWithGate(name, imgB, promUnhealthy, 15)
		waitPhase(name, "Paused", 4*time.Minute)
	})
})

// ---- helpers ----

// rolloutManifest renders a FleetRollout; a non-empty promURL adds a health gate.
func rolloutManifest(name, image, policy, promURL string, timeout int) string {
	gate := ""
	if promURL != "" {
		gate = fmt.Sprintf(`
  healthGate:
    prometheusURL: %s
    query: up
    timeoutSeconds: %d`, promURL, timeout)
	}
	return fmt.Sprintf(`apiVersion: fleet.fleetrollout.io/v1alpha1
kind: FleetRollout
metadata:
  name: %s
  namespace: %s
spec:
  targetSelector:
    matchLabels:
      fleet-group: field-robots
  image: %s
  waveSize: 1
  rollbackPolicy: %s%s
`, name, crNS, image, policy, gate)
}

func applyRollout(name, image, policy, promURL string, timeout int) {
	kubectlApply(rolloutManifest(name, image, policy, promURL, timeout))
}

// upgradeImage changes only the image (keeps whatever gate the CR already has).
func upgradeImage(name, image string) {
	_, err := utils.Run(exec.Command("kubectl", "patch", "fleetrollout", name, "-n", crNS,
		"--type", "merge", "-p", fmt.Sprintf(`{"spec":{"image":%q}}`, image)))
	Expect(err).NotTo(HaveOccurred(), "Failed to patch image")
}

// upgradeWithGate changes the image AND attaches a health gate in one edit (one generation bump).
func upgradeWithGate(name, image, promURL string, timeout int) {
	patch := fmt.Sprintf(`{"spec":{"image":%q,"healthGate":{"prometheusURL":%q,"query":"up","timeoutSeconds":%d}}}`,
		image, promURL, timeout)
	_, err := utils.Run(exec.Command("kubectl", "patch", "fleetrollout", name, "-n", crNS, "--type", "merge", "-p", patch))
	Expect(err).NotTo(HaveOccurred(), "Failed to patch image+gate")
}

func kubectlApply(manifest string) {
	f := filepath.Join(os.TempDir(), fmt.Sprintf("fr-%d.yaml", time.Now().UnixNano()))
	Expect(os.WriteFile(f, []byte(manifest), 0o644)).To(Succeed())
	defer func() { _ = os.Remove(f) }()
	_, err := utils.Run(exec.Command("kubectl", "apply", "-f", f))
	Expect(err).NotTo(HaveOccurred(), "kubectl apply failed for:\n%s", manifest)
}

func frField(name, jsonpath string) string {
	out, err := utils.Run(exec.Command("kubectl", "get", "fleetrollout", name, "-n", crNS, "-o", "jsonpath="+jsonpath))
	Expect(err).NotTo(HaveOccurred())
	return strings.TrimSpace(out)
}

func condReason(name, condType string) string {
	return frField(name, fmt.Sprintf(`{.status.conditions[?(@.type=="%s")].reason}`, condType))
}

func waitPhase(name, phase string, timeout time.Duration) {
	Eventually(func(g Gomega) {
		g.Expect(frField(name, "{.status.phase}")).To(Equal(phase))
	}, timeout, 5*time.Second).Should(Succeed(), "%s never reached phase %s", name, phase)
}

// podImages returns the sorted, de-duplicated set of container images across the rollout's pods.
func podImages(name string) []string {
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", crNS, "-l", ownerSelector+name,
		"-o", "jsonpath={range .items[*]}{.spec.containers[0].image}{\"\\n\"}{end}"))
	Expect(err).NotTo(HaveOccurred())
	set := map[string]struct{}{}
	for _, line := range utils.GetNonEmptyLines(out) {
		set[strings.TrimSpace(line)] = struct{}{}
	}
	images := make([]string, 0, len(set))
	for img := range set {
		images = append(images, img)
	}
	sort.Strings(images)
	return images
}
