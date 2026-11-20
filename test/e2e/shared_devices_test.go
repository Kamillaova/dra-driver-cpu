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
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	e2eclient "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/client"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	podmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/pod"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

// On a node using partitions the default partition's devices are shared: a
// claim on dra.cpu books an amount of one, and is capped at it. Elsewhere these
// specs skip.
var _ = ginkgo.Describe("Shared default devices", ginkgo.Label("partitions"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		restConfig        *rest.Config
		dracpuTesterImage string
		cfgValues         driverConfigValues
		targetNode        *v1.Node
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		restConfig, err = e2eclient.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create Kubernetes config")

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name)
		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset).effectiveFor(targetNode)
		if !cfgValues.partitionsUsed() {
			ginkgo.Skip("node " + targetNode.Name + " uses no CPU partitions, where dra.cpu gives exclusive CPUs; create the cluster with DRACPU_E2E_LANE=partitions")
		}
	})

	ginkgo.It("should publish the default partition's devices with a shared request policy, and map only them", func(ctx context.Context) {
		step := resource.MustParse("1")
		if cfgValues.AllowFractionalCapacityRequests {
			step = resource.MustParse("1m")
		}
		shared := 0
		for _, dev := range nodeDevices(ctx, gomega.Default, rootFxt.K8SClientset, targetNode.Name) {
			partition := dev.Attributes[device.AttributePartition].StringValue
			gomega.Expect(partition).ToNot(gomega.BeNil(), "device %s names no partition", dev.Name)
			if *partition != device.DefaultPartitionName {
				gomega.Expect(dev.NodeAllocatableResources).To(gomega.BeEmpty(), "device %s of partition %q carries a node allocatable mapping", dev.Name, *partition)
				continue
			}
			shared++
			gomega.Expect(dev.AllowMultipleAllocations).To(gomega.HaveValue(gomega.BeTrue()), "device %s does not allow several allocations", dev.Name)
			capacity, ok := dev.Capacity[resourcev1.QualifiedName(device.CPUResourceQualifiedName)]
			gomega.Expect(ok).To(gomega.BeTrue(), "device %s has no %s capacity", dev.Name, device.CPUResourceQualifiedName)
			policy := capacity.RequestPolicy
			gomega.Expect(policy).ToNot(gomega.BeNil(), "device %s has no request policy", dev.Name)
			gomega.Expect(policy.Default).ToNot(gomega.BeNil(), "device %s has no default amount", dev.Name)
			gomega.Expect(policy.Default.IsZero()).To(gomega.BeTrue(), "device %s books %s for a request naming no amount", dev.Name, policy.Default.String())
			gomega.Expect(policy.ValidRange).ToNot(gomega.BeNil(), "device %s has no valid range", dev.Name)
			gomega.Expect(policy.ValidRange.Min.IsZero()).To(gomega.BeTrue(), "device %s has minimum %s", dev.Name, policy.ValidRange.Min.String())
			gomega.Expect(policy.ValidRange.Max).ToNot(gomega.BeNil(), "device %s has no maximum", dev.Name)
			gomega.Expect(policy.ValidRange.Max.Cmp(capacity.Value)).To(gomega.BeZero(), "device %s has maximum %s, not its capacity %s", dev.Name, policy.ValidRange.Max.String(), capacity.Value.String())
			gomega.Expect(policy.ValidRange.Step).ToNot(gomega.BeNil(), "device %s has no step", dev.Name)
			gomega.Expect(policy.ValidRange.Step.Cmp(step)).To(gomega.BeZero(), "device %s has step %s, want %s", dev.Name, policy.ValidRange.Step.String(), step.String())
			if !cfgValues.PublishNodeAllocatableResourceMapping {
				gomega.Expect(dev.NodeAllocatableResources).To(gomega.BeEmpty(), "device %s carries a node allocatable mapping", dev.Name)
				continue
			}
			gomega.Expect(dev.NodeAllocatableResources).To(gomega.HaveKey(v1.ResourceCPU), "device %s has no cpu node allocatable mapping", dev.Name)
			mapping := dev.NodeAllocatableResources[v1.ResourceCPU].Mapping
			gomega.Expect(mapping).ToNot(gomega.BeNil(), "device %s: mapping must be set", dev.Name)
			gomega.Expect(mapping.CapacityKey).To(gomega.HaveValue(gomega.BeEquivalentTo(device.CPUResourceQualifiedName)), "device %s maps another capacity", dev.Name)
		}
		gomega.Expect(shared).To(gomega.BeNumerically(">", 0), "node %s publishes no device of partition %q", targetNode.Name, device.DefaultPartitionName)
	})

	ginkgo.It("should cap a share at its amount and give the amount in its metadata file", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-capped")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		spec := makeResourceClaimSpec(1, true)
		spec.Devices.Requests[0].Exactly.Capacity.Requests[device.CPUResourceQualifiedName] = resource.MustParse("0.5")
		pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, spec, "share-capped")

		gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUMax).To(gomega.Equal("50000 100000"),
			"a share of 0.5 CPU does not run under a quota of 50 ms each 100 ms period")
		expectShareMetadata(ctx, restConfig, fxt, pod, "500m", "500m")
	})

	ginkgo.It("should run a share that books no amount uncapped", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-uncapped")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		spec := makeResourceClaimSpec(1, true)
		spec.Devices.Requests[0].Exactly.Capacity = nil
		pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, spec, "share-uncapped")

		gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUMax).To(gomega.Equal("max 100000"),
			"a share that books nothing runs under a quota")
		expectShareMetadata(ctx, restConfig, fxt, pod, "0", "max")
	})

	for _, tc := range []struct {
		name, amount, limit, cpuMax, cpuLimit, cpuRequest string
	}{
		{name: "replace the amount with a written limit as the cap", amount: "500m", limit: "1", cpuMax: "100000 100000", cpuRequest: "500m", cpuLimit: "1"},
		{name: "run a share whose limit is max uncapped", amount: "500m", limit: "max", cpuMax: "max 100000", cpuRequest: "500m", cpuLimit: "max"},
		{name: "cap a share of 0 at its written limit", amount: "0", limit: "500m", cpuMax: "50000 100000", cpuRequest: "0", cpuLimit: "500m"},
	} {
		ginkgo.It("should "+tc.name, func(ctx context.Context) {
			fxt := rootFxt.WithPrefix("share-limit")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			spec := makeResourceClaimSpec(1, true)
			spec.Devices.Requests[0].Exactly.Capacity.Requests[device.CPUResourceQualifiedName] = resource.MustParse(tc.amount)
			withCPULimit(&spec, tc.limit)
			pod := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, spec, "share-limit")

			gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUMax).To(gomega.Equal(tc.cpuMax),
				"a share of %s with limit %s runs under another quota", tc.amount, tc.limit)
			expectShareMetadata(ctx, restConfig, fxt, pod, tc.cpuRequest, tc.cpuLimit)
		})
	}

	ginkgo.It("should refuse a container holding a second capped share", ginkgo.Label("negative"), func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-two-caps")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		var claims []*resourcev1.ResourceClaim
		for _, name := range []string{"share-a", "share-b"} {
			spec := makeResourceClaimSpec(1, true)
			spec.Devices.Requests[0].Exactly.Capacity.Requests[device.CPUResourceQualifiedName] = resource.MustParse("250m")
			claim, err := fxt.K8SClientset.ResourceV1().ResourceClaims(fxt.Namespace.Name).Create(ctx,
				&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fxt.Namespace.Name}, Spec: spec}, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create claim %s", name)
			claims = append(claims, claim)
		}
		pod := makeTesterPodWithClaimsWithoutCPURequest(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, claims...)
		pod, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create the tester pod")

		// The driver names a container's requests in the order of their claims' UIDs.
		slices.SortFunc(claims, func(a, b *resourcev1.ResourceClaim) int { return strings.Compare(string(a.UID), string(b.UID)) })
		want := fmt.Sprintf("container %q: request %q of claim %q and request %q of claim %q both have limits, so they cannot be used together: use the claims in separate containers, or remove one of the limits",
			pod.Spec.Containers[0].Name, "request-cpus", claims[0].Name, "request-cpus", claims[1].Name)
		EventuallyFailedToCreate(ctx, fxt, pod)
		gomega.Expect(createContainerError(ctx, fxt, pod)).To(gomega.ContainSubstring(want))
	})
})

// withCPULimit gives the spec's first request a cpuConfig.limit.
func withCPULimit(spec *resourcev1.ResourceClaimSpec, limit string) {
	ginkgo.GinkgoHelper()
	raw, err := json.Marshal(v1alpha1.OpaqueConfig{APIVersion: v1alpha1.APIVersion, CPUConfig: v1alpha1.CPUConfig{Limit: new(v1alpha1.Limit(limit))}})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	spec.Devices.Config = append(spec.Devices.Config, resourcev1.DeviceClaimConfiguration{
		Requests: []string{spec.Devices.Requests[0].Name},
		DeviceConfiguration: resourcev1.DeviceConfiguration{Opaque: &resourcev1.OpaqueDeviceConfiguration{
			Driver: driverName, Parameters: runtime.RawExtension{Raw: raw},
		}},
	})
}

// expectShareMetadata checks the metadata file of a pod's share: it names no
// CPUs and counts none, since a share's CPUs move, and gives the booked amount
// and the cap.
func expectShareMetadata(ctx context.Context, restConfig *rest.Config, fxt *fixture.Fixture, pod *v1.Pod, cpuRequest, cpuLimit string) {
	ginkgo.GinkgoHelper()
	devicesByRequest := readMetadataDevices(ctx, restConfig, fxt, pod)
	gomega.Expect(devicesByRequest).To(gomega.HaveKey("request-cpus"))
	gomega.Expect(devicesByRequest["request-cpus"]).ToNot(gomega.BeEmpty())
	for _, dev := range devicesByRequest["request-cpus"] {
		gomega.Expect(dev.Attributes).ToNot(gomega.HaveKey("dra.cpu/cpuset"), "a share's file names CPUs")
		gomega.Expect(dev.Attributes).ToNot(gomega.HaveKey("dra.cpu/allocatedNumCPUs"), "a share's file counts CPUs")
		expectStringAttrValue(dev.Attributes, "dra.cpu/cpuRequest", cpuRequest)
		expectStringAttrValue(dev.Attributes, "dra.cpu/cpuLimit", cpuLimit)
	}
}

// makeTesterPodWithClaimsWithoutCPURequest runs one tester container holding
// every claim, with no CPU request of its own.
func makeTesterPodWithClaimsWithoutCPURequest(ns, image, nodeName string, claims ...*resourcev1.ResourceClaim) *v1.Pod {
	ginkgo.GinkgoHelper()
	memQty, err := resource.ParseQuantity("256Mi")
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	pod := makeTesterPodWithExclusiveCPUClaimResources(ns, image, "", nodeName,
		v1.ResourceList{v1.ResourceMemory: memQty}, v1.ResourceList{v1.ResourceMemory: memQty})
	pod.Spec.Containers[0].Resources.Claims = nil
	pod.Spec.ResourceClaims = nil
	for idx, claim := range claims {
		name := fmt.Sprintf("cpu-claim-%d", idx)
		pod.Spec.Containers[0].Resources.Claims = append(pod.Spec.Containers[0].Resources.Claims, v1.ResourceClaim{Name: name})
		pod.Spec.ResourceClaims = append(pod.Spec.ResourceClaims, v1.PodResourceClaim{Name: name, ResourceClaimName: new(claim.Name)})
	}
	return pod
}

// createContainerError returns the message of the pod's container that waits
// with CreateContainerError, which carries the NRI plugin's refusal.
func createContainerError(ctx context.Context, fxt *fixture.Fixture, pod *v1.Pod) string {
	ginkgo.GinkgoHelper()
	current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	for _, status := range current.Status.ContainerStatuses {
		if status.State.Waiting != nil && status.State.Waiting.Reason == podmatchers.ReasonCreateContainerError {
			return status.State.Waiting.Message
		}
	}
	ginkgo.Fail(fmt.Sprintf("pod %s has no container waiting with %s", e2epod.Identify(current), podmatchers.ReasonCreateContainerError))
	return ""
}
