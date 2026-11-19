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
	"os"
	"slices"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	resourcehelper "k8s.io/component-helpers/resource"
)

// sharedDevice is one device of the default partition as the scheduler sees
// it.
type sharedDevice struct {
	node      string
	name      string
	capacity  int64 // millicores
	stepMilli int64
}

// The partitions lane gives worker2 a larger shared default device than the
// other nodes: its profile, e2e-wide, holds no reserved partition. Elsewhere no
// node publishes a device of partition default, and these specs skip.
var _ = ginkgo.Describe("Scheduling on shared default devices", ginkgo.Label("partitions"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		dracpuTesterImage string
		cfgValues         driverConfigValues
		byNode            map[string][]sharedDevice
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		byNode = sharedDevicesByNode(ctx, rootFxt.K8SClientset)
		if len(byNode) == 0 {
			ginkgo.Skip("no node publishes a device of partition " + device.DefaultPartitionName + "; create the cluster with DRACPU_E2E_LANE=partitions")
		}
	})

	// largestNode returns the node whose shared default device is the one
	// largest, and the largest shared device of any other node, in millicores.
	largestNode := func() (string, int64, int64, bool) {
		var best string
		var bestCapacity, others int64
		for _, node := range slices.Sorted(maps.Keys(byNode)) {
			capacity := maxCapacity(byNode[node])
			switch {
			case capacity > bestCapacity:
				others = max(others, bestCapacity)
				best, bestCapacity = node, capacity
			default:
				others = max(others, capacity)
			}
		}
		return best, bestCapacity, others, bestCapacity > others
	}

	ginkgo.It("should leave a share no shared device can hold Pending", ginkgo.Label("negative"), func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-impossible")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		var largest sharedDevice
		for _, devices := range byNode {
			for _, dev := range devices {
				if dev.capacity > largest.capacity {
					largest = dev
				}
			}
		}
		amount := largest.capacity + largest.stepMilli

		ginkgo.By(fmt.Sprintf("asking for %dm, one step above the largest shared device, %s on %s", amount, largest.name, largest.node))
		pod := createUnpinnedSharePod(ctx, fxt, dracpuTesterImage, amount, "share-impossible")
		gomega.Consistently(func(g gomega.Gomega) {
			current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(current.Status.Phase).To(gomega.Equal(v1.PodPending))
			g.Expect(current.Spec.NodeName).To(gomega.BeEmpty(), "a share no device can hold was bound to a node")
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should route a share only the largest shared device can hold to its node", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-route")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		node, capacity, others, ok := largestNode()
		if !ok {
			ginkgo.Skip(fmt.Sprintf("no node's shared default device is larger than every other node's (largest %dm)", capacity))
		}
		upper := capacity
		if cfgValues.PublishNodeAllocatableResourceMapping {
			upper = min(upper, freeAllocatableMilliCPU(ctx, fxt.K8SClientset, node))
		}
		step := byNode[node][0].stepMilli
		amount := (others + (upper-others)/2) / step * step
		if amount <= others {
			ginkgo.Skip(fmt.Sprintf("node %s can hold no share above %dm, the other nodes' largest shared device", node, others))
		}

		ginkgo.By(fmt.Sprintf("asking for %dm, which only node %s's shared device, of %dm, can hold", amount, node, capacity))
		pod := createUnpinnedSharePod(ctx, fxt, dracpuTesterImage, amount, "share-route")
		gomega.Expect(e2epod.WaitToBeRunning(ctx, fxt.K8SClientset, pod.Namespace, pod.Name)).To(gomega.Succeed())
		pod, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(pod.Spec.NodeName).To(gomega.Equal(node),
			"the scheduler placed a share of %dm on a node whose largest shared device holds %dm", amount, others)
		gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUMax).To(gomega.Equal(cpuMaxOf(amount)),
			"the share does not run capped at its amount")
	})

	ginkgo.It("should book no more of a node's shared devices than they hold under a burst of racing shares", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("share-burst")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		node, _, _, _ := largestNode()
		devices := byNode[node]
		var total int64
		for _, dev := range devices {
			total += dev.capacity
		}
		free := total
		if cfgValues.PublishNodeAllocatableResourceMapping {
			free = min(free, freeAllocatableMilliCPU(ctx, fxt.K8SClientset, node))
		}
		step := devices[0].stepMilli
		amount := free / 4 / step * step
		if amount == 0 {
			ginkgo.Skip(fmt.Sprintf("node %s has %dm to book, too little for a race", node, free))
		}
		var fit int64
		for _, dev := range devices {
			fit += dev.capacity / amount
		}
		fit = min(fit, free/amount)
		burst := fit + 2

		ginkgo.By(fmt.Sprintf("racing %d shares of %dm on node %s, which can book %d", burst, amount, node, fit))
		var pods []*v1.Pod
		for i := range burst {
			pods = append(pods, createSharePod(ctx, fxt, dracpuTesterImage, node, amount, fmt.Sprintf("share-burst-%d", i)))
		}
		running := func() []*v1.Pod {
			var out []*v1.Pod
			for _, pod := range pods {
				current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err == nil && current.Status.Phase == v1.PodRunning {
					out = append(out, current)
				}
			}
			return out
		}
		gomega.Eventually(func() int { return len(running()) }).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(gomega.BeEquivalentTo(fit))
		gomega.Consistently(func() int { return len(running()) }).WithTimeout(30*time.Second).WithPolling(5*time.Second).Should(gomega.BeEquivalentTo(fit),
			"more shares than the node can book are running")

		ginkgo.By("verifying no device is booked beyond its capacity, and every share runs capped")
		booked := map[string]int64{}
		for _, pod := range running() {
			for _, status := range pod.Status.ResourceClaimStatuses {
				gomega.Expect(status.ResourceClaimName).ToNot(gomega.BeNil(), "pod %s has no claim for %s", pod.Name, status.Name)
				claim, err := fxt.K8SClientset.ResourceV1().ResourceClaims(pod.Namespace).Get(ctx, *status.ResourceClaimName, metav1.GetOptions{})
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				gomega.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(), "claim %s of a running pod is not allocated", claim.Name)
				for _, result := range claim.Status.Allocation.Devices.Results {
					consumed := result.ConsumedCapacity[resourcev1.QualifiedName(device.CPUResourceQualifiedName)]
					booked[result.Device] += consumed.MilliValue()
				}
			}
			gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUMax).To(gomega.Equal(cpuMaxOf(amount)),
				"pod %s does not run capped at its share", pod.Name)
		}
		for _, dev := range devices {
			gomega.Expect(booked[dev.name]).To(gomega.BeNumerically("<=", dev.capacity),
				"device %s of %dm is booked %dm", dev.name, dev.capacity, booked[dev.name])
		}
	})
})

// sharedDevicesByNode returns each node's shared devices: those of the default
// partition that book from 0. Without partitions every device names the
// partition default and is owned.
func sharedDevicesByNode(ctx context.Context, cs kubernetes.Interface) map[string][]sharedDevice {
	ginkgo.GinkgoHelper()
	sliceList, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{
		FieldSelector: resourcev1.ResourceSliceSelectorDriver + "=" + driverName,
	})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot list the ResourceSlices")
	byNode := map[string][]sharedDevice{}
	for _, slice := range sliceList.Items {
		if slice.Spec.NodeName == nil {
			continue
		}
		for _, dev := range slice.Spec.Devices {
			partition := dev.Attributes[device.AttributePartition].StringValue
			if partition == nil || *partition != device.DefaultPartitionName {
				continue
			}
			capacity, ok := dev.Capacity[resourcev1.QualifiedName(device.CPUResourceQualifiedName)]
			gomega.Expect(ok).To(gomega.BeTrue(), "device %s has no %s capacity", dev.Name, device.CPUResourceQualifiedName)
			policy := capacity.RequestPolicy
			if policy == nil || policy.ValidRange == nil || policy.ValidRange.Min == nil || !policy.ValidRange.Min.IsZero() {
				continue
			}
			gomega.Expect(policy.ValidRange.Step).ToNot(gomega.BeNil(), "shared device %s has no step", dev.Name)
			byNode[*slice.Spec.NodeName] = append(byNode[*slice.Spec.NodeName], sharedDevice{
				node:      *slice.Spec.NodeName,
				name:      dev.Name,
				capacity:  capacity.Value.MilliValue(),
				stepMilli: policy.ValidRange.Step.MilliValue(),
			})
		}
	}
	return byNode
}

func maxCapacity(devices []sharedDevice) int64 {
	var largest int64
	for _, dev := range devices {
		largest = max(largest, dev.capacity)
	}
	return largest
}

// freeAllocatableMilliCPU is the node's allocatable CPU less what the pods
// bound to it request, as the scheduler counts it, the node allocatable
// mapping of their claims included.
func freeAllocatableMilliCPU(ctx context.Context, cs kubernetes.Interface, nodeName string) int64 {
	ginkgo.GinkgoHelper()
	node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	pods, err := cs.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + nodeName})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	free := node.Status.Allocatable.Cpu().MilliValue()
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == v1.PodSucceeded || pod.Status.Phase == v1.PodFailed {
			continue
		}
		requests := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{UseDRANodeAllocatableResourceClaimStatus: true})
		free -= requests.Cpu().MilliValue()
	}
	return free
}

// cpuMaxOf is the cpu.max the driver writes for a share capped at amount
// millicores, over a 100 ms period.
func cpuMaxOf(amount int64) string {
	return fmt.Sprintf("%d 100000", max(amount*100, 1000))
}

func shareClaimSpec(amount int64) resourcev1.ResourceClaimSpec {
	spec := makeResourceClaimSpec(1, true)
	spec.Devices.Requests[0].Exactly.Capacity.Requests[device.CPUResourceQualifiedName] = *resource.NewMilliQuantity(amount, resource.DecimalSI)
	return spec
}

// createSharePod creates, without waiting for it, a tester pod on nodeName
// holding a share of amount millicores.
func createSharePod(ctx context.Context, fxt *fixture.Fixture, image, nodeName string, amount int64, claimTemplateName string) *v1.Pod {
	ginkgo.GinkgoHelper()
	_, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, &resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: claimTemplateName},
		Spec:       resourcev1.ResourceClaimTemplateSpec{Spec: shareClaimSpec(amount)},
	}, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create claim template %s", claimTemplateName)
	pod := makeTesterPodWithExclusiveCPUClaimWithoutCPURequest(fxt.Namespace.Name, image, claimTemplateName, nodeName)
	if nodeName == "" {
		pod.Spec.NodeSelector = nil
	}
	pod, err = fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create a tester pod on claim template %s", claimTemplateName)
	return pod
}

// createUnpinnedSharePod leaves the node to the scheduler, which is the
// subject under test.
func createUnpinnedSharePod(ctx context.Context, fxt *fixture.Fixture, image string, amount int64, claimTemplateName string) *v1.Pod {
	ginkgo.GinkgoHelper()
	return createSharePod(ctx, fxt, image, "", amount, claimTemplateName)
}
