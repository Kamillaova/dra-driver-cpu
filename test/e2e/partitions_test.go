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
	"maps"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/discovery"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/cpuset"
)

// The partitions lane (DRACPU_E2E_LANE=partitions) labels every kind node with
// a profile. Elsewhere no node carries the label, and these specs skip.
var _ = ginkgo.Describe("CPU partitions", ginkgo.Label("partitions"), ginkgo.Serial, ginkgo.Ordered, func() {
	var (
		rootFxt           *fixture.Fixture
		dracpuTesterImage string
		cfgValues         driverConfigValues
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
	})

	ginkgo.It("should publish on each node the partitions of its own profile", func(ctx context.Context) {
		nodes, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list nodes")
		if len(nodes.Items) == 0 {
			ginkgo.Skip("no node carries the " + driverconfig.ProfileLabel + " label; create the cluster with DRACPU_E2E_LANE=partitions")
		}

		for i := range nodes.Items {
			node := &nodes.Items[i]
			want := map[string]string{}
			for _, partition := range cfgValues.effectiveFor(node).CPUPartitions {
				switch {
				case partition.Name == device.DefaultPartitionName:
					want[partition.Name] = device.PARTITION_ROLE_DEFAULT
				case partition.Role != device.PARTITION_ROLE_RESERVED:
					want[partition.Name] = partition.Role
				}
			}
			gomega.Eventually(func(g gomega.Gomega) map[string]string {
				return publishedPartitions(ctx, g, rootFxt.K8SClientset, node.Name)
			}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(gomega.Equal(want),
				"node %s with profile %q does not publish its profile's partitions", node.Name, node.Labels[driverconfig.ProfileLabel])
		}
	})

	// A single-thread partition needs cores whose siblings are offline, which
	// the lane declares only where the host already has them: no SMT, or
	// siblings an operator took offline. The suite never changes a host.
	ginkgo.Context("with a partition of one thread per core", func() {
		var (
			targetNode    *v1.Node
			partitionName string
			partitionCPUs cpuset.CPUSet
			coreOf        map[int]cpuset.CPUSet
		)

		ginkgo.BeforeAll(func(ctx context.Context) {
			var err error
			targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name)

			partition, ok := singleThreadPartition(cfgValues.effectiveFor(targetNode))
			if !ok {
				ginkgo.Skip("node " + targetNode.Name + " runs no partition of one thread per core; the partitions lane declares one where the host has a core without an online sibling")
			}
			partitionName = partition.Name
			partitionCPUs, err = cpuset.Parse(partition.CPUSet)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot parse the cpuset of partition %q", partitionName)

			infraFxt := rootFxt.WithPrefix("infra-partition")
			gomega.Expect(infraFxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(infraFxt.Teardown)
			infoPod := discovery.MakePod(infraFxt.Namespace.Name, dracpuTesterImage)
			infoPod = e2epod.PinToNode(infoPod, targetNode.Name)
			infoPod, err = e2epod.RunToCompletion(ctx, infraFxt.K8SClientset, infoPod)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot run the discovery pod")
			data, err := e2epod.GetLogs(ctx, infraFxt.K8SClientset, infoPod)
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get the discovery pod's logs")
			var cpuInfo discovery.DRACPUInfo
			gomega.Expect(unmarshalLatestReport(data, &cpuInfo)).To(gomega.Succeed())
			coreOf = coresOf(cpuInfo)
		})

		ginkgo.It("should publish devices of one thread per core for the partition", func(ctx context.Context) {
			devices := devicesOfPartition(ctx, rootFxt.K8SClientset, targetNode.Name, partitionName)
			gomega.Expect(devices).ToNot(gomega.BeEmpty(), "node %s publishes no device of partition %q", targetNode.Name, partitionName)
			for _, dev := range devices {
				gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeThreadsPerCore,
					resourcev1.DeviceAttribute{IntValue: new(int64(1))}), "device %s does not report one thread per core", dev.Name)
				gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeSMTEnabled,
					resourcev1.DeviceAttribute{BoolValue: new(false)}), "device %s reports SMT enabled", dev.Name)
				gomega.Expect(dev.Attributes).To(gomega.HaveKeyWithValue(device.AttributeRole,
					resourcev1.DeviceAttribute{StringValue: new(device.PARTITION_ROLE_EXCLUSIVE)}), "device %s is not exclusive", dev.Name)
			}
		})

		ginkgo.It("should give a claim on the partition's class CPUs of the partition without an online sibling", func(ctx context.Context) {
			fxt := rootFxt.WithPrefix("partition-claim")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			spec := makeResourceClaimSpec(1, true)
			spec.Devices.Requests[0].Exactly.DeviceClassName = driverName + "." + partitionName
			pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, spec, "cpu-claim-partition")

			assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
			gomega.Expect(assigned.Size()).To(gomega.Equal(1), "the claim for one CPU was given %s", assigned.String())
			gomega.Expect(assigned.IsSubsetOf(partitionCPUs)).To(gomega.BeTrue(),
				"the claim was given %s, outside partition %s (%s)", assigned.String(), partitionName, partitionCPUs.String())
			for _, cpu := range assigned.List() {
				gomega.Expect(coreOf).To(gomega.HaveKeyWithValue(cpu, cpusetmatchers.Equal(cpuset.New(cpu))),
					"CPU %d has an online sibling", cpu)
			}
		})

		ginkgo.It("should keep a container without a claim off the partition", func(ctx context.Context) {
			fxt := rootFxt.WithPrefix("partition-claimless")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			pod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)
			assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
			gomega.Expect(assigned.IsEmpty()).To(gomega.BeFalse(), "the container without a claim runs on no CPU")
			gomega.Expect(assigned).To(cpusetmatchers.HaveNoOverlapWith(partitionCPUs),
				"a container without a claim runs on partition %s", partitionName)
		})

		ginkgo.It("should give a claim on dra.cpu a share of the default partition", func(ctx context.Context) {
			fxt := rootFxt.WithPrefix("partition-share")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, makeResourceClaimSpec(1, true), "cpu-claim-default")
			reference := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)

			assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
			gomega.Expect(assigned).To(cpusetmatchers.HaveNoOverlapWith(partitionCPUs),
				"a claim of class %s runs on partition %s", driverName, partitionName)
			gomega.Expect(assigned).To(cpusetmatchers.Equal(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reference).CPUAssigned),
				"a claim of class %s does not share the CPUs of the containers without a claim", driverName)
		})
	})

	// The lane's kubelets reserve the CPUs outside each profile's default
	// partition, and e2e, with its reserved partition, needs more than
	// e2e-wide: a node moved from e2e-wide to e2e misses its reservation.
	ginkgo.Context("with the kubelet CPU reservation", func() {
		ginkgo.It("should publish devices and report the reservation on a node whose kubelet reserves its partitions' CPUs", func(ctx context.Context) {
			nodes, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list nodes")
			checked := 0
			for i := range nodes.Items {
				node := &nodes.Items[i]
				reserved := reservedCPUs(node)
				if reserved == 0 {
					continue
				}
				checked++
				gomega.Eventually(func(g gomega.Gomega) {
					current, err := rootFxt.K8SClientset.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
					g.Expect(err).ToNot(gomega.HaveOccurred())
					g.Expect(nodeCondition(current, conditionReservationMissing)).To(gomega.HaveValue(gomega.And(
						gomega.HaveField("Status", v1.ConditionFalse),
						gomega.HaveField("Reason", "SufficientCPUReservation"),
						gomega.HaveField("Message", fmt.Sprintf("kubelet reserves %d CPUs, %d required", reserved, reserved)),
					)), "node %s", node.Name)
					g.Expect(nodeDevices(ctx, g, rootFxt.K8SClientset, node.Name)).ToNot(gomega.BeEmpty(), "node %s publishes no device", node.Name)
				}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
			}
			if checked == 0 {
				ginkgo.Skip("no node's partitions need a kubelet CPU reservation; the partitions lane declares a reserved partition where the host leaves two CPUs to default")
			}
		})

		ginkgo.It("should publish nothing and report the missing reservation on a node whose kubelet reserves too little", ginkgo.Label("negative"), func(ctx context.Context) {
			short, needed, want, ok := nodeShortOfProfile(ctx, rootFxt.K8SClientset)
			if !ok {
				ginkgo.Skip("no node runs a profile needing less kubelet CPU reservation than another; the partitions lane gives e2e a reserved partition where the host leaves two CPUs to default")
			}
			fixture.By("moving node %s to profile %q, which needs a reservation of %d CPUs", short.Name, want, needed)
			restore := short.Labels[driverconfig.ProfileLabel]
			ginkgo.DeferCleanup(func(ctx context.Context) {
				relabelAndRestartDriver(ctx, rootFxt.K8SClientset, short.Name, restore)
				clearStoppedCondition(ctx, rootFxt.K8SClientset, short.Name, conditionReservationMissing)
				gomega.Eventually(func(g gomega.Gomega) {
					g.Expect(nodeDevices(ctx, g, rootFxt.K8SClientset, short.Name)).ToNot(gomega.BeEmpty())
				}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(gomega.Succeed(), "node %s publishes no device back on profile %q", short.Name, restore)
			})
			relabelAndRestartDriver(ctx, rootFxt.K8SClientset, short.Name, want)

			message := fmt.Sprintf("kubelet CPU reservation below %d: drain the node, then set `kubeReserved: {cpu: \"%d\"}` or `systemReserved: {cpu: \"%d\"}`; no %s devices are published until then",
				needed, needed, needed, driverName)
			gomega.Eventually(func(g gomega.Gomega) {
				current, err := rootFxt.K8SClientset.CoreV1().Nodes().Get(ctx, short.Name, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(nodeCondition(current, conditionReservationMissing)).To(gomega.HaveValue(gomega.And(
					gomega.HaveField("Status", v1.ConditionTrue),
					gomega.HaveField("Reason", "InsufficientCPUReservation"),
					gomega.HaveField("Message", message),
				)), "node %s", short.Name)
				g.Expect(nodeDevices(ctx, g, rootFxt.K8SClientset, short.Name)).To(gomega.BeEmpty(), "node %s publishes devices without its reservation", short.Name)
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
		})
	})

	// A driver writes the conditions it keeps Unknown as it stops, so a node
	// it left does not keep reporting what nobody checks any more.
	ginkgo.Context("when the driver leaves a node", func() {
		ginkgo.It("should turn the node conditions it keeps Unknown, and evaluate them again when it returns", func(ctx context.Context) {
			cs := rootFxt.K8SClientset
			node, err := e2enode.PickWorker(ctx, cs, 5*time.Second, 1*time.Minute, rootFxt.Log)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			if !cfgValues.effectiveFor(node).partitionsUsed() {
				ginkgo.Skip("node " + node.Name + " uses no CPU partitions; create the cluster with DRACPU_E2E_LANE=partitions")
			}
			kept := map[v1.NodeConditionType]string{conditionPartitionDegraded: "driver stopped; partitions not checked"}
			if cfgValues.PublishNodeAllocatableResourceMapping {
				kept[conditionAllocatableMappingDropped] = "driver stopped; mapping not checked"
			}
			if reservedCPUs(node) > 0 {
				kept[conditionReservationMissing] = "driver stopped; reservation not checked"
			}

			evaluated := map[v1.NodeConditionType]v1.NodeCondition{}
			gomega.Eventually(func(g gomega.Gomega) {
				current, err := cs.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				for conditionType := range kept {
					cond := nodeCondition(current, conditionType)
					g.Expect(cond).ToNot(gomega.BeNil(), "node %s has no %s condition", node.Name, conditionType)
					g.Expect(cond.Reason).ToNot(gomega.Equal(nodeConditionStoppedReason), "node %s: %s not evaluated", node.Name, conditionType)
					evaluated[conditionType] = *cond
				}
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())

			daemonSet, err := cs.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get dracpu daemonset")
			if daemonSet.Spec.Template.Spec.Affinity != nil {
				ginkgo.Skip("the dracpu DaemonSet carries an affinity of its own, which this spec would replace")
			}
			fixture.By("leaving node %s out of the dracpu DaemonSet", node.Name)
			ginkgo.DeferCleanup(restoreDaemonSetNodes, cs, node.Name)
			patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"NotIn","values":[%q]}]}]}}}}}}}`, node.Name)
			_, err = cs.AppsV1().DaemonSets(daemonSetNamespace).Patch(ctx, "dracpu", types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot patch the dracpu DaemonSet")
			gomega.Eventually(func(g gomega.Gomega) {
				pods, err := cs.CoreV1().Pods(daemonSetNamespace).List(ctx, metav1.ListOptions{LabelSelector: daemonSetLabel, FieldSelector: "spec.nodeName=" + node.Name})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(pods.Items).To(gomega.BeEmpty(), "a driver pod is still on node %s", node.Name)
			}).WithTimeout(driverPodPollTimeout).WithPolling(driverPodPollInterval).Should(gomega.Succeed())

			current, err := cs.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			for conditionType, stopped := range kept {
				gomega.Expect(nodeCondition(current, conditionType)).To(gomega.HaveValue(gomega.And(
					gomega.HaveField("Status", v1.ConditionUnknown),
					gomega.HaveField("Reason", nodeConditionStoppedReason),
					gomega.HaveField("Message", stopped),
				)), "node %s after its driver left", node.Name)
			}

			fixture.By("giving node %s its driver back", node.Name)
			restoreDaemonSetNodes(ctx, cs, node.Name)
			gomega.Eventually(func(g gomega.Gomega) {
				current, err := cs.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				for conditionType, before := range evaluated {
					g.Expect(nodeCondition(current, conditionType)).To(gomega.HaveValue(gomega.And(
						gomega.HaveField("Status", before.Status),
						gomega.HaveField("Reason", before.Reason),
						gomega.HaveField("Message", before.Message),
					)), "node %s after its driver returned", node.Name)
				}
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
		})
	})

	// e2e-grown gives the CPUs of e2e-wide's single-thread partition to a
	// reserved partition: a node moved to it with a claim prepared there has a
	// reserved partition holding the claim's CPUs.
	ginkgo.Context("with a reserved partition grown over a prepared claim", func() {
		ginkgo.It("should stop the claim's containers and refuse them until the claim's pod is deleted", func(ctx context.Context) {
			cs := rootFxt.K8SClientset
			grown, ok := cfgValues.Profiles[grownProfile]
			if !ok {
				ginkgo.Skip("the configuration declares no profile " + grownProfile + "; the partitions lane declares it where the host has a core without an online sibling")
			}
			nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel + "=" + wideProfile})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list nodes")
			gomega.Expect(nodes.Items).ToNot(gomega.BeEmpty(), "no node runs profile %s", wideProfile)
			node := &nodes.Items[0]
			partition, ok := singleThreadPartition(cfgValues.effectiveFor(node))
			gomega.Expect(ok).To(gomega.BeTrue(), "profile %s holds no single-thread partition beside %s", wideProfile, grownProfile)
			gomega.Expect(grown.CPUPartitions).To(gomega.ContainElement(gomega.And(
				gomega.HaveField("Name", grownPartition),
				gomega.HaveField("Role", device.PARTITION_ROLE_RESERVED),
				gomega.HaveField("CPUSet", partition.CPUSet),
			)), "profile %s does not reserve the CPUs of partition %s", grownProfile, partition.Name)
			if !kindNodeReachable(ctx, node.Name) {
				ginkgo.Skip("docker reaches no container of node " + node.Name + ", whose containerd the spec restarts; run it on a kind cluster")
			}

			ginkgo.DeferCleanup(relabelAndRestartDriver, cs, node.Name, wideProfile)
			fxt := rootFxt.WithPrefix("grown-reserved")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)
			spec := makeResourceClaimSpec(1, true)
			spec.Devices.Requests[0].Exactly.DeviceClassName = driverName + "." + partition.Name
			pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, node.Name, spec, "grown-claim")
			assigned := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
			gomega.Expect(pod.Status.ResourceClaimStatuses).To(gomega.HaveLen(1))
			claimName := *pod.Status.ResourceClaimStatuses[0].ResourceClaimName

			fixture.By("moving node %s to profile %s, whose reserved partition %s holds CPUs %s of claim %s", node.Name, grownProfile, grownPartition, assigned.String(), claimName)
			relabelAndRestartDriver(ctx, cs, node.Name, grownProfile)
			refusal := fmt.Sprintf("reserved partition %q now holds CPUs %s of prepared claim %s/%s (request %q); its containers are stopped: delete the claim's pod or keep those CPUs out of the partition",
				grownPartition, assigned.String(), pod.Namespace, claimName, "request-cpus")
			expectPartitionDegraded(ctx, cs, node.Name, v1.ConditionTrue, "ReservedHoldsClaims", refusal)
			gomega.Eventually(func(g gomega.Gomega) {
				current, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(current.Status.ContainerStatuses).To(gomega.ContainElement(gomega.HaveField("LastTerminationState.Terminated",
					gomega.HaveValue(gomega.HaveField("ExitCode", int32(137))))), "the claim's container was not killed")
			}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
			created := fmt.Sprintf("cannot create container %q in pod %q: %s", pod.Spec.Containers[0].Name, pod.UID, refusal)
			expectPodEvent(ctx, cs, pod, "Failed", created)
			expectDriverHealthz(ctx, cs, node.Name, http.StatusServiceUnavailable)

			fixture.By("restarting containerd in node %s while partition %s holds the claim", node.Name, grownPartition)
			driverPod, err := e2epod.GetDRACPUPod(ctx, cs, node.Name)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			restarted := metav1.Now()
			restartContainerd(ctx, node.Name)
			expectDriverSynchronizedSince(ctx, cs, driverPod, restarted)
			gomega.Consistently(func(g gomega.Gomega) {
				current, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				g.Expect(err).ToNot(gomega.HaveOccurred())
				for _, status := range current.Status.ContainerStatuses {
					g.Expect(status.State.Running).To(gomega.BeNil(), "the claim's container runs while partition %s holds its CPUs", grownPartition)
				}
			}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(gomega.Succeed())
			expectDriverHealthz(ctx, cs, node.Name, http.StatusServiceUnavailable)

			fixture.By("deleting the claim's pod")
			gomega.Expect(e2epod.DeleteSync(ctx, cs, pod)).To(gomega.Succeed())
			expectDriverHealthz(ctx, cs, node.Name, http.StatusOK)
			expectPartitionDegraded(ctx, cs, node.Name, v1.ConditionFalse, "PartitionsVerified", "the kernel has online the threads per core every CPU partition's smt declares")
		})
	})
})

const (
	conditionReservationMissing        v1.NodeConditionType = "CPUReservationMissing"
	conditionPartitionDegraded         v1.NodeConditionType = "CPUPartitionDegraded"
	conditionAllocatableMappingDropped v1.NodeConditionType = "CPUAllocatableMappingDropped"
	nodeConditionStoppedReason                              = "DriverStopped"
)

// reservedCPUs is the kubelet's CPU reservation on node, capacity less
// allocatable, which the lane sets in whole CPUs.
func reservedCPUs(node *v1.Node) int64 {
	ginkgo.GinkgoHelper()
	reserved := node.Status.Capacity.Cpu().MilliValue() - node.Status.Allocatable.Cpu().MilliValue()
	gomega.Expect(reserved%1000).To(gomega.BeZero(), "node %s reserves %dm, not whole CPUs", node.Name, reserved)
	return reserved / 1000
}

// nodeShortOfProfile finds a labelled node whose kubelet reserves less than
// another profile's nodes do, and returns it, the reservation that profile
// needs, and the profile.
func nodeShortOfProfile(ctx context.Context, cs kubernetes.Interface) (*v1.Node, int64, string, bool) {
	ginkgo.GinkgoHelper()
	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list nodes")
	needs := map[string]int64{}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		needs[node.Labels[driverconfig.ProfileLabel]] = max(needs[node.Labels[driverconfig.ProfileLabel]], reservedCPUs(node))
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		for _, profile := range slices.Sorted(maps.Keys(needs)) {
			if needs[profile] > reservedCPUs(node) {
				return node, needs[profile], profile, true
			}
		}
	}
	return nil, 0, "", false
}

func nodeCondition(node *v1.Node, conditionType v1.NodeConditionType) *v1.NodeCondition {
	for i := range node.Status.Conditions {
		if node.Status.Conditions[i].Type == conditionType {
			return &node.Status.Conditions[i]
		}
	}
	return nil
}

// relabelAndRestartDriver selects profile on the node and restarts the driver
// there, which reads the label only when it starts.
func relabelAndRestartDriver(ctx context.Context, cs kubernetes.Interface, nodeName, profile string) {
	ginkgo.GinkgoHelper()
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, driverconfig.ProfileLabel, profile)
	_, err := cs.CoreV1().Nodes().Patch(ctx, nodeName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot label node %s with profile %q", nodeName, profile)
	restartDriverPod(ctx, cs, nodeName)
}

// restartDriverPod deletes the driver's pod on the node and waits for the
// DaemonSet's next one to run.
func restartDriverPod(ctx context.Context, cs kubernetes.Interface, nodeName string) {
	ginkgo.GinkgoHelper()
	old, err := e2epod.GetDRACPUPod(ctx, cs, nodeName)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	gomega.Expect(cs.CoreV1().Pods(old.Namespace).Delete(ctx, old.Name, metav1.DeleteOptions{})).To(gomega.Succeed())
	gomega.Eventually(func(g gomega.Gomega) {
		pods, err := cs.CoreV1().Pods(old.Namespace).List(ctx, metav1.ListOptions{LabelSelector: daemonSetLabel, FieldSelector: "spec.nodeName=" + nodeName})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(pods.Items).To(gomega.ContainElement(gomega.And(
			gomega.HaveField("UID", gomega.Not(gomega.Equal(old.UID))),
			gomega.HaveField("Status.Phase", v1.PodRunning),
		)), "no new driver pod runs on node %s", nodeName)
	}).WithTimeout(driverPodPollTimeout).WithPolling(driverPodPollInterval).Should(gomega.Succeed())
}

// clearStoppedCondition removes a condition the driver wrote Unknown when it
// stopped, and that the driver now running no longer keeps.
func clearStoppedCondition(ctx context.Context, cs kubernetes.Interface, nodeName string, conditionType v1.NodeConditionType) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		idx := slices.IndexFunc(node.Status.Conditions, func(c v1.NodeCondition) bool { return c.Type == conditionType })
		if idx < 0 || node.Status.Conditions[idx].Reason != "DriverStopped" {
			return
		}
		patch := fmt.Sprintf(`[{"op":"test","path":"/status/conditions/%d/type","value":%q},{"op":"remove","path":"/status/conditions/%d"}]`, idx, conditionType, idx)
		_, err = cs.CoreV1().Nodes().Patch(ctx, nodeName, types.JSONPatchType, []byte(patch), metav1.PatchOptions{}, "status")
		g.Expect(err).ToNot(gomega.HaveOccurred())
	}).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
}

const (
	wideProfile    = "e2e-wide"
	grownProfile   = "e2e-grown"
	grownPartition = "grown"
)

func expectPartitionDegraded(ctx context.Context, cs kubernetes.Interface, nodeName string, status v1.ConditionStatus, reason, message string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(nodeCondition(node, conditionPartitionDegraded)).To(gomega.HaveValue(gomega.And(
			gomega.HaveField("Status", status),
			gomega.HaveField("Reason", reason),
			gomega.HaveField("Message", message),
		)), "node %s", nodeName)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
}

// expectPodEvent waits for an event of reason on pod whose message holds
// text, as the kubelet posts a failed container create with its error.
func expectPodEvent(ctx context.Context, cs kubernetes.Interface, pod *v1.Pod, reason, text string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		events, err := cs.CoreV1().Events(pod.Namespace).List(ctx, metav1.ListOptions{
			FieldSelector: "involvedObject.kind=Pod,involvedObject.name=" + pod.Name + ",reason=" + reason,
		})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(events.Items).To(gomega.ContainElement(gomega.HaveField("Message", gomega.ContainSubstring(text))),
			"pod %s has no %s event holding %q", pod.Name, reason, text)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
}

func expectDriverHealthz(ctx context.Context, cs kubernetes.Interface, nodeName string, want int) {
	ginkgo.GinkgoHelper()
	httpClient := &http.Client{Timeout: 10 * time.Second}
	gomega.Eventually(func(g gomega.Gomega) {
		driverPod, err := e2epod.GetDRACPUPod(ctx, cs, nodeName)
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(driverPod.Status.PodIP).ToNot(gomega.BeEmpty())
		resp, err := httpClient.Get(healthzURL(driverPod.Status.PodIP)) //nolint:noctx // httpClient.Timeout covers this
		g.Expect(err).ToNot(gomega.HaveOccurred())
		defer resp.Body.Close()
		g.Expect(resp.StatusCode).To(gomega.Equal(want), "/healthz of the driver on node %s", nodeName)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
}

// kindNodeReachable reports whether docker runs a container named as the
// node, which kind nodes are.
func kindNodeReachable(ctx context.Context, nodeName string) bool {
	return exec.CommandContext(ctx, "docker", "inspect", "--type", "container", nodeName).Run() == nil
}

func restartContainerd(ctx context.Context, nodeName string) {
	ginkgo.GinkgoHelper()
	out, err := exec.CommandContext(ctx, "docker", "exec", nodeName, "systemctl", "restart", "containerd").CombinedOutput()
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot restart containerd in node %s: %s", nodeName, out)
}

// expectDriverSynchronizedSince waits for the driver to log the end of a
// Synchronize after since, which the runtime sends once the driver has
// connected to NRI again.
func expectDriverSynchronizedSince(ctx context.Context, cs kubernetes.Interface, driverPod *v1.Pod, since metav1.Time) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func(g gomega.Gomega) {
		logs, err := cs.CoreV1().Pods(driverPod.Namespace).GetLogs(driverPod.Name, &v1.PodLogOptions{SinceTime: &since}).DoRaw(ctx)
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(string(logs)).To(gomega.ContainSubstring("end: synchronize state with the runtime"),
			"the driver on node %s logged no Synchronize since %s", driverPod.Spec.NodeName, since)
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
	current, err := cs.CoreV1().Pods(driverPod.Namespace).Get(ctx, driverPod.Name, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	for i, status := range current.Status.ContainerStatuses {
		gomega.Expect(status.RestartCount).To(gomega.Equal(driverPod.Status.ContainerStatuses[i].RestartCount),
			"the driver on node %s restarted instead of connecting to NRI again", driverPod.Spec.NodeName)
	}
}

// restoreDaemonSetNodes drops the affinity the spec gave the dracpu DaemonSet,
// and waits for its driver to run on the node again and the rollout to end.
func restoreDaemonSetNodes(ctx context.Context, cs kubernetes.Interface, nodeName string) {
	ginkgo.GinkgoHelper()
	daemonSet, err := cs.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get dracpu daemonset")
	if daemonSet.Spec.Template.Spec.Affinity != nil {
		_, err = cs.AppsV1().DaemonSets(daemonSetNamespace).Patch(ctx, "dracpu", types.JSONPatchType,
			[]byte(`[{"op":"remove","path":"/spec/template/spec/affinity"}]`), metav1.PatchOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot restore the dracpu DaemonSet")
	}
	gomega.Eventually(func(g gomega.Gomega) {
		daemonSet, err := cs.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(daemonSet.Status.ObservedGeneration).To(gomega.BeNumerically(">=", daemonSet.Generation))
		g.Expect(daemonSet.Status.UpdatedNumberScheduled).To(gomega.Equal(daemonSet.Status.DesiredNumberScheduled))
		g.Expect(daemonSet.Status.NumberAvailable).To(gomega.Equal(daemonSet.Status.DesiredNumberScheduled))
		pods, err := cs.CoreV1().Pods(daemonSetNamespace).List(ctx, metav1.ListOptions{LabelSelector: daemonSetLabel, FieldSelector: "spec.nodeName=" + nodeName})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(pods.Items).To(gomega.ContainElement(gomega.HaveField("Status.Phase", v1.PodRunning)), "no driver pod runs on node %s", nodeName)
	}).WithTimeout(5 * time.Minute).WithPolling(driverPodPollInterval).Should(gomega.Succeed())
}

// singleThreadPartition returns the first partition of v whose smt keeps one
// online thread per core.
func singleThreadPartition(v driverConfigValues) (driverconfig.CPUPartition, bool) {
	for _, partition := range v.CPUPartitions {
		if partition.SMT != nil && partition.SMT.Threads == 1 {
			return partition, true
		}
	}
	return driverconfig.CPUPartition{}, false
}

func nodeDevices(ctx context.Context, g gomega.Gomega, cs kubernetes.Interface, nodeName string) []resourcev1.Device {
	sliceList, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{
		FieldSelector: resourcev1.ResourceSliceSelectorDriver + "=" + driverName + "," + resourcev1.ResourceSliceSelectorNodeName + "=" + nodeName,
	})
	g.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list the ResourceSlices of node %s", nodeName)
	var devices []resourcev1.Device
	for _, slice := range sliceList.Items {
		devices = append(devices, slice.Spec.Devices...)
	}
	return devices
}

// publishedPartitions maps each partition the node publishes a device of to
// that partition's role.
func publishedPartitions(ctx context.Context, g gomega.Gomega, cs kubernetes.Interface, nodeName string) map[string]string {
	published := map[string]string{}
	for _, dev := range nodeDevices(ctx, g, cs, nodeName) {
		name, role := dev.Attributes[device.AttributePartition].StringValue, dev.Attributes[device.AttributeRole].StringValue
		g.Expect(name).ToNot(gomega.BeNil(), "device %s names no partition", dev.Name)
		g.Expect(role).ToNot(gomega.BeNil(), "device %s names no role", dev.Name)
		published[*name] = *role
	}
	return published
}

func devicesOfPartition(ctx context.Context, cs kubernetes.Interface, nodeName, partitionName string) []resourcev1.Device {
	ginkgo.GinkgoHelper()
	var devices []resourcev1.Device
	for _, dev := range nodeDevices(ctx, gomega.Default, cs, nodeName) {
		if name := dev.Attributes[device.AttributePartition].StringValue; name != nil && *name == partitionName {
			devices = append(devices, dev)
		}
	}
	return devices
}

func createTesterPodWithClaimSpec(ctx context.Context, fxt *fixture.Fixture, image, nodeName string, spec resourcev1.ResourceClaimSpec, claimTemplateName string) *v1.Pod {
	ginkgo.GinkgoHelper()
	template := &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: claimTemplateName},
		Spec:       resourcev1.ResourceClaimTemplateSpec{Spec: spec},
	}
	pod := makeTesterPodWithExclusiveCPUClaimWithoutCPURequest(fxt.Namespace.Name, image, claimTemplateName, nodeName)
	created, _, err := e2epod.CreateSyncWithClaimTemplate(ctx, fxt.K8SClientset, template, pod)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot run a tester pod on claim template %s", claimTemplateName)
	return created
}
