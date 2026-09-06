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
	"os"
	"sort"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/cpuset"
	"k8s.io/utils/ptr"
)

// fleetDevice is one published device of one node, as the scheduler sees it.
type fleetDevice struct {
	node          string
	name          string
	capacity      int
	cachesInGroup int
	step          int
}

// discoverFleet reads every dra.cpu ResourceSlice in the cluster. The result is
// the scheduler's whole view: per-device capacity, geometry attributes, and the
// request policy step. It says nothing about which CPUs are free or where.
func discoverFleet(ctx context.Context, cs kubernetes.Interface) []fleetDevice {
	ginkgo.GinkgoHelper()
	slices, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	var fleet []fleetDevice
	for _, slice := range slices.Items {
		if slice.Spec.Driver != driverName || slice.Spec.NodeName == nil {
			continue
		}
		for _, dev := range slice.Spec.Devices {
			capacity, ok := dev.Capacity["dra.cpu/cpu"]
			if !ok {
				continue
			}
			fd := fleetDevice{node: *slice.Spec.NodeName, name: dev.Name, capacity: int(capacity.Value.Value()), step: 1}
			if capacity.RequestPolicy != nil && capacity.RequestPolicy.ValidRange != nil && capacity.RequestPolicy.ValidRange.Step != nil {
				fd.step = int(capacity.RequestPolicy.ValidRange.Step.Value())
			}
			if attr, ok := dev.Attributes["dra.cpu/uncoreCachesInGroup"]; ok && attr.IntValue != nil {
				fd.cachesInGroup = int(*attr.IntValue)
			}
			fleet = append(fleet, fd)
		}
	}
	return fleet
}

// makeUnpinnedClaimPod consumes a claim template without naming a node: where
// it runs is entirely the scheduler's choice, which is the subject under test.
func makeUnpinnedClaimPod(ns, image, claimTemplateName string) *v1.Pod {
	memory := resource.MustParse("64Mi")
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "tester-pod-sched-", Namespace: ns},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:    "tester-container-1",
				Image:   image,
				Command: []string{"/dracputester"},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceMemory: memory},
					Limits:   v1.ResourceList{v1.ResourceMemory: memory},
					Claims:   []v1.ResourceClaim{{Name: "cpu"}},
				},
			}},
			ResourceClaims: []v1.PodResourceClaim{
				{Name: "cpu", ResourceClaimTemplateName: ptr.To(claimTemplateName)},
			},
			RestartPolicy: v1.RestartPolicyAlways,
		},
	}
}

func createClaimTemplate(ctx context.Context, fxt *fixture.Fixture, name string, spec resourcev1.ResourceClaimSpec) {
	ginkgo.GinkgoHelper()
	template := resourcev1.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       resourcev1.ResourceClaimTemplateSpec{Spec: spec},
	}
	_, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, &template, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
}

func roundUpTo(n, step int) int {
	if step <= 1 {
		return n
	}
	return ((n + step - 1) / step) * step
}

var _ = ginkgo.Describe("Cross-node scheduling", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		dracpuTesterImage string
		cfgValues         driverConfigValues
		fleet             []fleetDevice
		nodes             []string
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)

		fleet = discoverFleet(ctx, rootFxt.K8SClientset)
		seen := map[string]bool{}
		for _, dev := range fleet {
			if !seen[dev.node] {
				seen[dev.node] = true
				nodes = append(nodes, dev.node)
			}
		}
		sort.Strings(nodes)
		rootFxt.Log.Info("fleet discovered", "nodes", nodes, "devices", len(fleet))
		if len(nodes) < 2 {
			ginkgo.Skip(fmt.Sprintf("cross-node scheduling needs at least two nodes publishing %s devices, found %v", driverName, nodes))
		}
	})

	maxDeviceOf := func(node string) fleetDevice {
		best := fleetDevice{}
		for _, dev := range fleet {
			if dev.node == node && dev.capacity > best.capacity {
				best = dev
			}
		}
		return best
	}

	ginkgo.It("should route a claim past nodes whose devices cannot hold it", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("sched-route")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		big := fleetDevice{}
		for _, node := range nodes {
			if dev := maxDeviceOf(node); dev.capacity > big.capacity {
				big = dev
			}
		}
		secondBest := 0
		for _, node := range nodes {
			if node == big.node {
				continue
			}
			if dev := maxDeviceOf(node); dev.capacity > secondBest {
				secondBest = dev.capacity
			}
		}
		size := secondBest + big.step
		if size > big.capacity {
			ginkgo.Skip(fmt.Sprintf("the fleet's device capacities (%d vs %d) leave no size only one node can hold", secondBest, big.capacity))
		}

		ginkgo.By(fmt.Sprintf("requesting %d CPUs, which only %s on %s can hold", size, big.name, big.node))
		createClaimTemplate(ctx, fxt, "cpu-claim-route", makeResourceClaimSpec(size, cfgValues.CPUDeviceMode == "grouped"))
		pod := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-route")
		pod, err := e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		gomega.Expect(pod.Spec.NodeName).To(gomega.Equal(big.node),
			"the scheduler placed a %d-CPU claim on a node whose largest device holds %d", size, secondBest)

		ginkgo.By("verifying the container actually holds that many CPUs")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		gomega.Expect(alloc.CPUAssigned).To(cpusetmatchers.HaveSize(roundUpTo(size, big.step)))
	})

	ginkgo.It("should leave a claim no device can hold Pending, not misplace it", ginkgo.Label("negative"), func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("sched-impossible")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		biggest, step := 0, 1
		for _, dev := range fleet {
			if dev.capacity > biggest {
				biggest, step = dev.capacity, dev.step
			}
		}
		size := biggest + step

		ginkgo.By(fmt.Sprintf("requesting %d CPUs, one step above the fleet's largest device", size))
		createClaimTemplate(ctx, fxt, "cpu-claim-impossible", makeResourceClaimSpec(size, cfgValues.CPUDeviceMode == "grouped"))
		pod := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-impossible")
		pod, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		gomega.Consistently(func(g gomega.Gomega) {
			reread, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(reread.Status.Phase).To(gomega.Equal(v1.PodPending))
			g.Expect(reread.Spec.NodeName).To(gomega.BeEmpty(), "an unsatisfiable claim was bound to a node")
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())

		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, pod)).To(gomega.Succeed())
	})

	ginkgo.It("should honour cache-geometry selectors across a heterogeneous fleet", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("sched-geometry")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		minCaches, maxCaches := fleet[0].cachesInGroup, fleet[0].cachesInGroup
		for _, dev := range fleet {
			minCaches = min(minCaches, dev.cachesInGroup)
			maxCaches = max(maxCaches, dev.cachesInGroup)
		}
		if minCaches == maxCaches || minCaches == 0 {
			ginkgo.Skip(fmt.Sprintf("every device reports %d uncore caches, geometry selectors cannot discriminate", maxCaches))
		}
		manyCacheNodes, fewCacheNodes := map[string]bool{}, map[string]bool{}
		for _, dev := range fleet {
			if dev.cachesInGroup >= maxCaches {
				manyCacheNodes[dev.node] = true
			}
			if dev.cachesInGroup <= minCaches {
				fewCacheNodes[dev.node] = true
			}
		}

		step := 1
		for _, dev := range fleet {
			step = max(step, dev.step)
		}

		ginkgo.By(fmt.Sprintf("steering one claim to devices with >= %d caches and one to devices with <= %d", maxCaches, minCaches))
		celMany := fmt.Sprintf(`device.attributes["dra.cpu"].uncoreCachesInGroup >= %d`, maxCaches)
		celFew := fmt.Sprintf(`device.attributes["dra.cpu"].uncoreCachesInGroup <= %d`, minCaches)
		createClaimTemplate(ctx, fxt, "cpu-claim-many-caches", claimSpecWithSelector(step, celMany))
		createClaimTemplate(ctx, fxt, "cpu-claim-few-caches", claimSpecWithSelector(step, celFew))

		podMany := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-many-caches")
		podMany, err := e2epod.CreateSync(ctx, fxt.K8SClientset, podMany)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		podFew := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-few-caches")
		podFew, err = e2epod.CreateSync(ctx, fxt.K8SClientset, podFew)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		gomega.Expect(manyCacheNodes).To(gomega.HaveKey(podMany.Spec.NodeName),
			"the >= %d caches claim landed on %s, whose devices do not qualify", maxCaches, podMany.Spec.NodeName)
		gomega.Expect(fewCacheNodes).To(gomega.HaveKey(podFew.Spec.NodeName),
			"the <= %d caches claim landed on %s, whose devices do not qualify", minCaches, podFew.Spec.NodeName)
	})

	ginkgo.It("should never over-allocate under a burst of racing claims", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("sched-burst")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		// One node, every claim racing for it: the worst case for the
		// scheduler's DRA capacity accounting. The claims fit only until the
		// devices are full, and not one CPU may be promised twice.
		target := ""
		for _, node := range nodes {
			if target == "" || maxDeviceOf(node).capacity > maxDeviceOf(target).capacity {
				target = node
			}
		}
		step := maxDeviceOf(target).step
		size := 2 * step
		fit := 0
		for _, dev := range fleet {
			if dev.node == target {
				fit += dev.capacity / size
			}
		}
		burst := fit + 3
		if fit < 2 {
			ginkgo.Skip(fmt.Sprintf("node %s fits only %d claims of %d CPUs, not enough for a race", target, fit, size))
		}

		ginkgo.By(fmt.Sprintf("bursting %d claims of %d CPUs at %s, which fits %d", burst, size, target, fit))
		createClaimTemplate(ctx, fxt, "cpu-claim-burst", makeResourceClaimSpec(size, cfgValues.CPUDeviceMode == "grouped"))
		var pods []*v1.Pod
		for range burst {
			pod := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-burst")
			pod = e2epod.PinToNode(pod, target)
			pod, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			pods = append(pods, pod)
		}

		running := func() []*v1.Pod {
			var out []*v1.Pod
			for _, pod := range pods {
				reread, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
				if err == nil && reread.Status.Phase == v1.PodRunning {
					out = append(out, reread)
				}
			}
			return out
		}
		gomega.Eventually(func() int { return len(running()) }, 10*time.Minute, 5*time.Second).Should(gomega.Equal(fit))
		gomega.Consistently(func() int { return len(running()) }, 30*time.Second, 5*time.Second).Should(gomega.Equal(fit),
			"more claims than the node's capacity ended up Running")

		ginkgo.By("verifying not one CPU was promised twice")
		union := cpuset.New()
		for _, pod := range running() {
			cpus := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod).CPUAssigned
			gomega.Expect(cpus).To(cpusetmatchers.HaveSize(size))
			gomega.Expect(cpus).To(cpusetmatchers.HaveNoOverlapWith(union),
				"pod %s overlaps another claim's CPUs", pod.Name)
			union = union.Union(cpus)
		}
		gomega.Expect(union).To(cpusetmatchers.HaveSize(fit * size))
	})

	ginkgo.It("should have defragmentation repair a claim the scheduler lands on fragmented free space", func(ctx context.Context) {
		// The scheduler sees one free-capacity number per device, never its
		// shape: a device whose free CPUs are scattered one slice per cache
		// looks identical to one holding a whole cache. This is the documented
		// blind spot -- the node cannot be chosen better, only repaired after.
		if !cfgValues.DefragEnabled {
			ginkgo.Skip("defragmentation is not enabled in the driver configuration")
		}
		fxt := rootFxt.WithPrefix("sched-blindspot")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		minCaches, maxCaches := fleet[0].cachesInGroup, fleet[0].cachesInGroup
		for _, dev := range fleet {
			minCaches = min(minCaches, dev.cachesInGroup)
			maxCaches = max(maxCaches, dev.cachesInGroup)
		}
		if maxCaches < 2 {
			ginkgo.Skip("no device spans two uncore caches, nothing can fragment")
		}
		target := ""
		for _, dev := range fleet {
			if dev.cachesInGroup == maxCaches {
				target = dev.node
			}
		}
		cel := ""
		if minCaches != maxCaches {
			cel = fmt.Sprintf(`device.attributes["dra.cpu"].uncoreCachesInGroup >= %d`, maxCaches)
		}

		step := allocationStep(ctx, fxt.K8SClientset, target)
		baseline, err := getPlacements(ctx, fxt.K8SClientset, target, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		if len(free) < 2 || free[0] < 3*step {
			ginkgo.Skip(fmt.Sprintf("fragmenting needs at least two caches with %d or more free CPUs each, got %v", 3*step, free))
		}
		scopeCEL := numaCEL(cfgValues, fragNUMA)
		if cel != "" && scopeCEL != "" {
			scopeCEL = cel + " && " + scopeCEL
		} else if scopeCEL == "" {
			scopeCEL = cel
		}

		ginkgo.By(fmt.Sprintf("checkerboarding NUMA node %d of the target node with pinned fillers", fragNUMA))
		var fillers []*v1.Pod
		for i, cacheFree := range free {
			pod, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, target,
				claimSpecWithSelector(cacheFree-2*step, scopeCEL), fmt.Sprintf("cpu-claim-bs-filler-%d", i))
			if err != nil {
				break
			}
			fillers = append(fillers, pod)
		}
		if len(fillers) < 2 {
			ginkgo.Skip(fmt.Sprintf("could only place %d of %d fillers", len(fillers), len(free)))
		}

		ginkgo.By("letting the scheduler place a claim no single cache can hold")
		createClaimTemplate(ctx, fxt, "cpu-claim-bs-victim", claimSpecWithSelector(3*step, scopeCEL))
		victim := makeUnpinnedClaimPod(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-bs-victim")
		victim, err = e2epod.CreateSync(ctx, fxt.K8SClientset, victim)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(victim.Spec.NodeName).To(gomega.Equal(target),
			"the claim was steered at the fragmented node and landed elsewhere")

		// The gate must be the victim's own spread: total excess can belong to
		// a straddling filler, which the driver may repair without touching the
		// victim -- correctly, and this spec would have nothing to show.
		var victimUID string
		claims, err := fxt.K8SClientset.ResourceV1().ResourceClaims(fxt.Namespace.Name).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		for _, claim := range claims.Items {
			for _, ref := range claim.OwnerReferences {
				if ref.UID == victim.UID {
					victimUID = string(claim.UID)
				}
			}
		}
		gomega.Expect(victimUID).ToNot(gomega.BeEmpty(), "no claim owned by the victim pod")

		fragmented, err := getPlacements(ctx, fxt.K8SClientset, target, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		before, ok := fragmented.claimCPUs(victimUID)
		gomega.Expect(ok).To(gomega.BeTrue(), "the victim claim is not reported: %+v", fragmented.Claims)
		if spreadOf(fragmented, before) < 2 {
			ginkgo.Skip(fmt.Sprintf("the victim landed unsplit on %s; nothing to repair", before.String()))
		}

		ginkgo.By("releasing a filler and waiting for the repair the scheduler could not make")
		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, fillers[0])).To(gomega.Succeed())
		var settled placementsReport
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, target, true)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(report.plannedMoves()).To(gomega.BeZero())
			g.Expect(report.totalExcess()).To(gomega.BeZero(),
				"claims still span more caches than their sizes require: %+v", report.NUMANodes)
			settled = report
		}, 3*time.Minute, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("verifying the repair unsplit the claim, kept its size, and restarted nothing")
		after, ok := settled.claimCPUs(victimUID)
		gomega.Expect(ok).To(gomega.BeTrue(), "the victim claim vanished: %+v", settled.Claims)
		gomega.Expect(spreadOf(settled, after)).To(gomega.Equal(1), "the claim is still split: %s", after.String())
		gomega.Expect(after).To(cpusetmatchers.HaveSize(before.Size()))
		gomega.Expect(after).ToNot(cpusetmatchers.Equal(before), "an unsplit claim cannot keep its old scattered CPUs")

		reread, err := fxt.K8SClientset.CoreV1().Pods(victim.Namespace).Get(ctx, victim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero())
		// The tester reports every five seconds, so the line right after the
		// final commit can still show the pre-move cpuset.
		gomega.Eventually(func(g gomega.Gomega) {
			live := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread).CPUAssigned
			g.Expect(live).To(cpusetmatchers.Equal(after), "the container is not on the CPUs the driver says its claim holds")
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())
	})
})
