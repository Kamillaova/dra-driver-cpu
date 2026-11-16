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
	"os"
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
})

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
