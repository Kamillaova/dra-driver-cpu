/*
Copyright The Kubernetes Authors.

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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The gates-off lane (DRACPU_E2E_LANE=gates-off) turns DRANodeAllocatableResources
// and DRADeviceTaints off in every component and installs the driver as the
// partitions lane does. Elsewhere the API server runs the gates on, and these
// specs skip.
var _ = ginkgo.Describe("Feature gates off", ginkgo.Label("gates-off"), ginkgo.Serial, ginkgo.Ordered, func() {
	var (
		rootFxt   *fixture.Fixture
		cfgValues driverConfigValues
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
	})

	ginkgo.It("should report the node-allocatable mapping dropped on every node that publishes it", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		skipUnlessAPIServerGateOff(ctx, cs, "DRANodeAllocatableResources")
		if !cfgValues.PublishNodeAllocatableResourceMapping {
			ginkgo.Skip("the driver publishes no node-allocatable mapping")
		}
		pods, err := cs.CoreV1().Pods(daemonSetNamespace).List(ctx, metav1.ListOptions{LabelSelector: daemonSetLabel})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list the driver's pods")
		gomega.Expect(pods.Items).ToNot(gomega.BeEmpty(), "no driver pod runs")
		for _, pod := range pods.Items {
			nodeName := pod.Spec.NodeName
			gomega.Eventually(func(g gomega.Gomega) {
				node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(nodeCondition(node, conditionAllocatableMappingDropped)).To(gomega.HaveValue(gomega.And(
					gomega.HaveField("Status", v1.ConditionTrue),
					gomega.HaveField("Reason", "DroppedByAPIServer"),
					gomega.HaveField("Message", "API server dropped nodeAllocatableResources from dra.cpu ResourceSlices, so dra.cpu claims are not counted against node cpu; enable DRANodeAllocatableResources on the API server, then restart the driver pod"),
				)), "node %s", nodeName)
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
			for _, dev := range nodeDevices(ctx, gomega.Default, cs, nodeName) {
				gomega.Expect(dev.NodeAllocatableResources).To(gomega.BeEmpty(), "node %s: device %s keeps the mapping", nodeName, dev.Name)
			}
		}
	})

	ginkgo.It("should withhold the degraded partition's devices whose taint the API server dropped", func(ctx context.Context) {
		cs := rootFxt.K8SClientset
		skipUnlessAPIServerGateOff(ctx, cs, "DRADeviceTaints")
		targetNode, err := e2enode.PickWorker(ctx, cs, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if _, ok := partitionNamed(cfgValues.effectiveFor(targetNode), degradedPartition); !ok {
			ginkgo.Skip("node " + targetNode.Name + " runs no partition " + degradedPartition + "; the lane declares one where the host has a core with an online sibling")
		}
		if !cfgValues.TaintDegradedPartitions {
			ginkgo.Skip("the driver runs without taintDegradedPartitions, so partition " + degradedPartition + " publishes no devices whatever the gate")
		}

		gomega.Eventually(func(g gomega.Gomega) map[string]string {
			return publishedPartitions(ctx, g, cs, targetNode.Name)
		}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(gomega.And(
			gomega.HaveKey(device.DefaultPartitionName),
			gomega.Not(gomega.HaveKey(degradedPartition)),
		), "node %s publishes partition %s, whose taint the API server drops", targetNode.Name, degradedPartition)
		gomega.Consistently(func(g gomega.Gomega) map[string]string {
			return publishedPartitions(ctx, g, cs, targetNode.Name)
		}).WithTimeout(20*time.Second).WithPolling(5*time.Second).ShouldNot(gomega.HaveKey(degradedPartition),
			"node %s published partition %s again", targetNode.Name, degradedPartition)
	})
})

// skipUnlessAPIServerGateOff skips the spec unless the API server reports the
// feature gate off in its kubernetes_feature_enabled metric.
func skipUnlessAPIServerGateOff(ctx context.Context, cs kubernetes.Interface, gate string) {
	ginkgo.GinkgoHelper()
	raw, err := cs.CoreV1().RESTClient().Get().AbsPath("/metrics").DoRaw(ctx)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot read the API server's metrics")
	prefix := fmt.Sprintf("kubernetes_feature_enabled{name=%q,", gate)
	for line := range strings.Lines(string(raw)) {
		if strings.HasPrefix(line, prefix) {
			if strings.HasSuffix(strings.TrimSpace(line), " 0") {
				return
			}
			break
		}
	}
	ginkgo.Skip("the API server runs " + gate + " on; create the cluster with DRACPU_E2E_LANE=gates-off")
}
