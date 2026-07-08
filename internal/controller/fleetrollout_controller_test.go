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
					ObjectMeta: metav1.ObjectMeta{Name: n, Labels: map[string]string{"fleet-group": "field-robots"}},
				}
				Expect(k8sClient.Create(ctx, node)).To(Succeed())
				node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
				Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
			}

			By("creating the FleetRollout")
			fr := &fleetv1alpha1.FleetRollout{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
				Spec: fleetv1alpha1.FleetRolloutSpec{
					TargetSelector: metav1.LabelSelector{MatchLabels: map[string]string{"fleet-group": "field-robots"}},
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
})
