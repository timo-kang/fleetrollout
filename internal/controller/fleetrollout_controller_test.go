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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

var _ = Describe("FleetRollout Controller", func() {
	Context("When reconciling a FleetRollout with matching Ready nodes", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
			image             = "registry.k8s.io/pause:3.9"
		)
		ctx := context.Background()
		key := types.NamespacedName{Name: resourceName, Namespace: resourceNamespace}
		nodeNames := []string{"fr-node-1", "fr-node-2"}

		BeforeEach(func() {
			By("creating two Ready nodes labeled for the fleet")
			for _, n := range nodeNames {
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: n, Labels: map[string]string{fleetGroupKey: fleetGroupVal}},
				}
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
				Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
			}

			By("creating the FleetRollout")
			fr := &fleetv1alpha1.FleetRollout{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: fleetv1alpha1.FleetRolloutSpec{
					TargetSelector: metav1.LabelSelector{MatchLabels: map[string]string{fleetGroupKey: fleetGroupVal}},
					Image:          image,
					WaveSize:       intstr.FromString("50%"),
				},
			}
			Expect(k8sClient.Create(ctx, fr)).To(Succeed())
		})

		AfterEach(func() {
			fr := &fleetv1alpha1.FleetRollout{}
			if err := k8sClient.Get(ctx, key, fr); err == nil {
				Expect(k8sClient.Delete(ctx, fr)).To(Succeed())
			}
			_ = k8sClient.Delete(ctx, &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace}})
			for _, n := range nodeNames {
				_ = k8sClient.Delete(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n}})
			}
		})

		It("creates an owned OnDelete DaemonSet targeting the fleet", func() {
			r := &FleetRolloutReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			ds := &appsv1.DaemonSet{}
			Expect(k8sClient.Get(ctx, key, ds)).To(Succeed())

			By("using the spec image and OnDelete strategy")
			Expect(ds.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(ds.Spec.Template.Spec.Containers[0].Image).To(Equal(image))
			Expect(ds.Spec.UpdateStrategy.Type).To(Equal(appsv1.OnDeleteDaemonSetStrategyType))

			By("targeting the selected nodes")
			Expect(ds.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("fleet-group", "field-robots"))

			By("being owned (controller ref) by the FleetRollout for GC")
			Expect(ds.OwnerReferences).To(HaveLen(1))
			Expect(ds.OwnerReferences[0].Kind).To(Equal("FleetRollout"))
			Expect(ds.OwnerReferences[0].Name).To(Equal(resourceName))
			Expect(ds.OwnerReferences[0].Controller).ToNot(BeNil())
			Expect(*ds.OwnerReferences[0].Controller).To(BeTrue())

			By("reporting total waves in status")
			fr := &fleetv1alpha1.FleetRollout{}
			Expect(k8sClient.Get(ctx, key, fr)).To(Succeed())
			// 2 nodes, waveSize "50%" → ceil(2*0.5)=1 node/wave → 2 waves.
			Expect(fr.Status.TotalWaves).To(Equal(int32(2)))
		})
	})

	// CEL / schema validation is enforced by the real API server in envtest.
	Context("Spec validation (CEL)", func() {
		const ns = "default"
		sel := metav1.LabelSelector{MatchLabels: map[string]string{"fleet-group": "field-robots"}}
		base := func() *fleetv1alpha1.FleetRollout {
			return &fleetv1alpha1.FleetRollout{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "cel-", Namespace: ns},
				Spec:       fleetv1alpha1.FleetRolloutSpec{TargetSelector: sel},
			}
		}
		gate := func(url, query string, timeout int32) *fleetv1alpha1.HealthGate {
			return &fleetv1alpha1.HealthGate{PrometheusURL: url, Query: query, TimeoutSeconds: timeout}
		}
		ctx := context.Background()

		DescribeTable("rejects invalid specs and accepts valid ones",
			func(mutate func(*fleetv1alpha1.FleetRollout), wantAccept bool) {
				fr := base()
				mutate(fr)
				err := k8sClient.Create(ctx, fr)
				if wantAccept {
					Expect(err).NotTo(HaveOccurred())
					Expect(k8sClient.Delete(ctx, fr)).To(Succeed())
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			Entry("neither image nor template", func(fr *fleetv1alpha1.FleetRollout) {}, false),
			Entry("both image and template", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: agentContainer, Image: frImage}}}}
			}, false),
			Entry("template with user nodeSelector", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"foo": "bar"},
					Containers:   []corev1.Container{{Name: agentContainer, Image: frImage}}}}
			}, false),
			Entry("waveSize 0%", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.WaveSize = intstr.FromString("0%")
			}, false),
			Entry("waveSize abc%", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.WaveSize = intstr.FromString("abc%")
			}, false),
			Entry("waveSize 20% valid", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.WaveSize = intstr.FromString("20%")
			}, true),
			Entry("negative timeout", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.HealthGate = gate("http://p:9090", "up", -5)
			}, false),
			Entry("ftp prometheusURL (SSRF)", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.HealthGate = gate("ftp://evil/x", "up", 60)
			}, false),
			Entry("valid https healthGate", func(fr *fleetv1alpha1.FleetRollout) {
				fr.Spec.Image = frImage
				fr.Spec.HealthGate = gate("https://p:9090", "up", 60)
			}, true),
		)
	})
})
