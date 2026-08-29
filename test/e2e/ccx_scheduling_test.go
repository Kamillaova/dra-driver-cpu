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
	"sort"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/cpuset"
)

const (
	fitAnnotationName = "dra.cpu/fit"
	// ccxSchedulerName is the schedulerName the CCXAlign profile serves. A pod
	// naming it is scheduled by that second scheduler and by nothing else, so
	// its absence leaves the pod Pending rather than falling back.
	ccxSchedulerName = "dracpu-scheduler"
)

// fitAnnotation mirrors the payload the driver publishes. Mirrored rather than
// imported because the reader that matters lives in another repository and
// cannot import this one either: what is asserted here is the wire format.
type fitAnnotation struct {
	V      int    `json:"v"`
	Policy string `json:"policy"`
	NUMA   []struct {
		ID               int   `json:"id"`
		CacheCPUs        []int `json:"cacheCPUs"`
		FreeCPUs         []int `json:"freeCPUs"`
		RepackedFreeCPUs []int `json:"repackedFreeCPUs"`
	} `json:"numaNodes"`
}

func (f fitAnnotation) on(numaID int) (int, bool) {
	for i, numa := range f.NUMA {
		if numa.ID == numaID {
			return i, true
		}
	}
	return 0, false
}

// largestFree is the biggest claim a NUMA node could take inside one cache, in
// whichever of the two views is asked for.
func largestOf(values []int) int {
	largest := 0
	for _, value := range values {
		if value > largest {
			largest = value
		}
	}
	return largest
}

// nodeIsSchedulable reports whether an untolerated pod could be placed here.
func nodeIsSchedulable(ctx context.Context, cs kubernetes.Interface, nodeName string) (bool, error) {
	node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if node.Spec.Unschedulable {
		return false, nil
	}
	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule || taint.Effect == v1.TaintEffectNoExecute {
			return false, nil
		}
	}
	return true, nil
}

func getFitAnnotation(ctx context.Context, cs kubernetes.Interface, nodeName string) (fitAnnotation, bool) {
	ginkgo.GinkgoHelper()
	var fit fitAnnotation
	node, err := cs.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	raw, ok := node.Annotations[fitAnnotationName]
	if !ok {
		return fit, false
	}
	gomega.Expect(json.Unmarshal([]byte(raw), &fit)).To(gomega.Succeed(), "the driver published unparseable JSON: %s", raw)
	return fit, true
}

// freePerCacheInOrder is the driver's own per-cache free counts for one NUMA
// node, in cache order. Unlike freePerCacheOn it does not sort: the annotation
// is index-aligned to this order, and that alignment is the thing under test.
func (r placementsReport) freePerCacheInOrder(numaID int) []int {
	var free []int
	for _, node := range r.NUMANodes {
		if node.NUMANodeID != numaID {
			continue
		}
		for _, cache := range node.Caches {
			cpus, err := cpuset.Parse(cache.FreeCPUs)
			if err != nil {
				continue
			}
			free = append(free, cpus.Size())
		}
	}
	return free
}

// cachesThatCanHold counts the node's uncore caches with room for a claim of
// this size, across every NUMA node.
func cachesThatCanHold(ctx context.Context, cs kubernetes.Interface, nodeName string, size int) int {
	ginkgo.GinkgoHelper()
	fit, ok := getFitAnnotation(ctx, cs, nodeName)
	gomega.Expect(ok).To(gomega.BeTrue())
	count := 0
	for _, numa := range fit.NUMA {
		for _, free := range numa.FreeCPUs {
			if free >= size {
				count++
			}
		}
	}
	return count
}

// leaveOneCacheFor narrows a node to a single cache that can still hold size,
// while leaving it plenty of CPUs elsewhere.
//
// Each filler takes half of size, which under either placement policy lands in
// a cache nothing has broken into yet and leaves it too small for the claim --
// so the count of caches that fit falls by one per filler, and the CPUs the
// filler did not take stay free. That surplus is the point. Were the caches
// consumed outright, a node holding one claim would have too little capacity
// left for a second and the scheduler's own filter would exclude it, so the
// spec would pass whether or not the score had noticed anything. Leaving the
// node able to hold a second claim but unable to align it is what puts the
// question to CCXAlign.
//
// A filler is pinned to the NUMA node it is counted against, since a claim
// never spans one.
func leaveOneCacheFor(ctx context.Context, fxt *fixture.Fixture, image, nodeName string, cfg driverConfigValues, size int, prefix string) {
	ginkgo.GinkgoHelper()
	for i := range 64 {
		fit, ok := getFitAnnotation(ctx, fxt.K8SClientset, nodeName)
		gomega.Expect(ok).To(gomega.BeTrue())

		total, target := 0, -1
		for _, numa := range fit.NUMA {
			for _, free := range numa.FreeCPUs {
				if free >= size {
					total++
					if target < 0 {
						target = numa.ID
					}
				}
			}
		}
		if total <= 1 {
			return
		}
		_, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, image, nodeName,
			claimSpecWithSelector(size/2, numaCEL(cfg, target)), fmt.Sprintf("%s-%d", prefix, i))
		if err != nil {
			fxt.Log.Info("stopped consuming caches", "node", nodeName, "reason", err.Error())
			return
		}
	}
}

// makeUnpinnedClaimPodOn is makeUnpinnedClaimPod for a named scheduler.
func makeUnpinnedClaimPodOn(ns, image, claimTemplateName, schedulerName string) *v1.Pod {
	pod := makeUnpinnedClaimPod(ns, image, claimTemplateName)
	pod.Spec.SchedulerName = schedulerName
	return pod
}

// ccxSchedulerPresent probes for the second scheduler by asking it to schedule
// something, rather than by recognising a Deployment: what the specs depend on
// is that a pod naming it gets placed, and no shape of manifest proves that.
func ccxSchedulerPresent(ctx context.Context, fxt *fixture.Fixture) bool {
	ginkgo.GinkgoHelper()
	memory := resource.MustParse("32Mi")
	probe := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "ccx-scheduler-probe-", Namespace: fxt.Namespace.Name},
		Spec: v1.PodSpec{
			SchedulerName: ccxSchedulerName,
			Containers: []v1.Container{{
				Name:    "probe",
				Image:   os.Getenv("DRACPU_E2E_TEST_IMAGE"),
				Command: []string{"/dracputester"},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceMemory: memory},
					Limits:   v1.ResourceList{v1.ResourceMemory: memory},
				},
			}},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	probe, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, probe, metav1.CreateOptions{})
	if err != nil {
		return false
	}
	defer func() {
		_ = fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Delete(context.Background(), probe.Name, metav1.DeleteOptions{})
	}()

	placed := false
	gomega.Eventually(func() bool {
		current, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Get(ctx, probe.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		placed = current.Spec.NodeName != ""
		return placed
	}).WithTimeout(45*time.Second).WithPolling(2*time.Second).Should(gomega.BeTrue(),
		"no scheduler answered for schedulerName %q", ccxSchedulerName)
	return placed
}

var _ = ginkgo.Describe("CCX-aligned scheduling", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		dracpuTesterImage string
		cfgValues         driverConfigValues
		nodes             []string
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		if !cfgValues.PublishFitAnnotation {
			ginkgo.Skip("the driver is not publishing " + fitAnnotationName)
		}

		seen := map[string]bool{}
		for _, dev := range discoverFleet(ctx, rootFxt.K8SClientset) {
			if seen[dev.node] {
				continue
			}
			seen[dev.node] = true
			// Only nodes the scheduler may actually choose. A control plane
			// publishes devices like any other node, but a NoSchedule taint
			// puts it out of reach of a pod carrying no toleration -- and
			// these specs are about which node gets chosen.
			if schedulable, err := nodeIsSchedulable(ctx, rootFxt.K8SClientset, dev.node); err == nil && schedulable {
				nodes = append(nodes, dev.node)
			}
		}
		sort.Strings(nodes)
		if len(nodes) < 2 {
			ginkgo.Skip(fmt.Sprintf("CCX-aligned scheduling needs at least two schedulable nodes publishing %s devices, found %v", driverName, nodes))
		}
	})

	// The annotation is the only thing the scheduler sees, and /placements is
	// what the driver itself believes. A disagreement between them is a bug
	// nothing downstream could detect: the scheduler would simply make good
	// decisions about a node that is not there.
	ginkgo.It("should publish a shape matching the driver's own view of the node", func(ctx context.Context) {
		compared := 0
		for _, node := range nodes {
			// /placements is served on the driver pod's own IP. A runner that
			// cannot route to it -- a fleet whose nodes span machines, say --
			// has no vantage on this node and says nothing about it, rather
			// than reporting a driver fault it never observed.
			if _, err := getPlacements(ctx, rootFxt.K8SClientset, node, false); err != nil {
				rootFxt.Log.Info("no vantage on this node's driver, not comparing", "node", node, "reason", err.Error())
				continue
			}
			compared++

			gomega.Eventually(func(g gomega.Gomega) {
				fit, ok := getFitAnnotation(ctx, rootFxt.K8SClientset, node)
				g.Expect(ok).To(gomega.BeTrue(), "node %s publishes no %s", node, fitAnnotationName)
				g.Expect(fit.V).To(gomega.Equal(1))
				g.Expect(fit.Policy).To(gomega.Equal(cfgValues.effectivePlacementPolicy()))

				current, err := getPlacements(ctx, rootFxt.K8SClientset, node, false)
				g.Expect(err).ToNot(gomega.HaveOccurred())
				for _, numa := range current.NUMANodes {
					idx, found := fit.on(numa.NUMANodeID)
					g.Expect(found).To(gomega.BeTrue(), "NUMA node %d is missing from %s on %s", numa.NUMANodeID, fitAnnotationName, node)
					entry := fit.NUMA[idx]
					g.Expect(entry.FreeCPUs).To(gomega.Equal(current.freePerCacheInOrder(numa.NUMANodeID)),
						"per-cache free CPUs disagree on %s NUMA node %d", node, numa.NUMANodeID)

					// What the reader validates before it will use the payload
					// at all: it discards the whole node on any of these.
					g.Expect(entry.CacheCPUs).To(gomega.HaveLen(len(entry.FreeCPUs)))
					g.Expect(entry.RepackedFreeCPUs).To(gomega.HaveLen(len(entry.FreeCPUs)))
					freeSum, repackedSum := 0, 0
					for i, capacity := range entry.CacheCPUs {
						g.Expect(entry.FreeCPUs[i]).To(gomega.BeNumerically("<=", capacity))
						g.Expect(entry.RepackedFreeCPUs[i]).To(gomega.BeNumerically("<=", capacity))
						freeSum += entry.FreeCPUs[i]
						repackedSum += entry.RepackedFreeCPUs[i]
					}
					g.Expect(freeSum).To(gomega.BeNumerically("<=", repackedSum))
				}
			}).WithTimeout(90 * time.Second).WithPolling(5 * time.Second).Should(gomega.Succeed())
			rootFxt.Log.Info("fit annotation agrees with the driver", "node", node)
		}
		gomega.Expect(compared).To(gomega.BeNumerically(">", 0),
			"no node's driver was reachable from this runner, so nothing was verified")
	})

	ginkgo.It("should route a whole-cache claim to the node that can align it", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("ccx-route")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)
		if !ccxSchedulerPresent(ctx, fxt) {
			ginkgo.Skip("the CCXAlign scheduler is not deployed")
		}

		// One node is spoiled; every other stays clean. Which clean node wins
		// is not the assertion -- on a fleet of three the tiers are equal
		// across all of them and the default scorers break the tie. What the
		// tiers promise is that the spoiled one loses.
		spoiled, clean := nodes[0], nodes[1]
		report, err := getPlacements(ctx, fxt.K8SClientset, spoiled, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		numaID := report.numaWithMostCaches()

		cleanFit, ok := getFitAnnotation(ctx, fxt.K8SClientset, clean)
		gomega.Expect(ok).To(gomega.BeTrue())
		wholeCache := 0
		for _, numa := range cleanFit.NUMA {
			if free := largestOf(numa.FreeCPUs); free > wholeCache {
				wholeCache = free
			}
		}
		if wholeCache == 0 {
			ginkgo.Skip(fmt.Sprintf("node %s has no whole cache free to route a claim to", clean))
		}

		ginkgo.By(fmt.Sprintf("leaving %s with no cache able to hold %d CPUs", spoiled, wholeCache))
		fillers := fillCachesDownTo(ctx, fxt, dracpuTesterImage, spoiled, cfgValues, numaID, 2, "ccx-fill")
		if len(fillers) == 0 {
			ginkgo.Skip(fmt.Sprintf("could not fragment %s", spoiled))
		}

		spoiledFit, ok := getFitAnnotation(ctx, fxt.K8SClientset, spoiled)
		gomega.Expect(ok).To(gomega.BeTrue())
		for _, numa := range spoiledFit.NUMA {
			if numa.ID != numaID {
				continue
			}
			if largestOf(numa.RepackedFreeCPUs) >= wholeCache {
				ginkgo.Skip(fmt.Sprintf("%s can still align %d CPUs after a repack, so there is nothing to route past", spoiled, wholeCache))
			}
		}

		ginkgo.By(fmt.Sprintf("scheduling a %d-CPU claim with no node named", wholeCache))
		createClaimTemplate(ctx, fxt, "cpu-claim-ccx", makeResourceClaimSpec(wholeCache, cfgValues.CPUDeviceMode == "grouped"))
		pod := makeUnpinnedClaimPodOn(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-ccx", ccxSchedulerName)
		pod, err = e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		gomega.Expect(pod.Spec.NodeName).ToNot(gomega.Equal(spoiled),
			"a claim needing a whole cache was placed on %s, which cannot align it even after a repack", spoiled)
		fxt.Log.Info("whole-cache claim routed away from the fragmented node", "landedOn", pod.Spec.NodeName, "avoided", spoiled)
	})

	ginkgo.It("should still place a claim no node can align, rather than leaving it Pending", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("ccx-fallback")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)
		if !ccxSchedulerPresent(ctx, fxt) {
			ginkgo.Skip("the CCXAlign scheduler is not deployed")
		}

		size := 0
		for _, node := range nodes {
			report, err := getPlacements(ctx, fxt.K8SClientset, node, false)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			numaID := report.numaWithMostCaches()
			ginkgo.By(fmt.Sprintf("fragmenting %s", node))
			fillCachesDownTo(ctx, fxt, dracpuTesterImage, node, cfgValues, numaID, 2, "ccx-fb-"+node)

			fit, ok := getFitAnnotation(ctx, fxt.K8SClientset, node)
			gomega.Expect(ok).To(gomega.BeTrue())
			for _, numa := range fit.NUMA {
				if free := largestOf(numa.FreeCPUs); free > size {
					size = free
				}
			}
		}
		// Bigger than any single cache can hold anywhere, so no node scores
		// better than split -- which is a preference, never a veto.
		size += 2
		if size <= 2 {
			ginkgo.Skip("the fleet has no free CPUs left to place a claim on")
		}

		ginkgo.By(fmt.Sprintf("scheduling a %d-CPU claim no node can align", size))
		createClaimTemplate(ctx, fxt, "cpu-claim-fb", makeResourceClaimSpec(size, cfgValues.CPUDeviceMode == "grouped"))
		pod := makeUnpinnedClaimPodOn(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-fb", ccxSchedulerName)
		pod, err := e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(),
			"a score plugin must never make a pod unschedulable; fragmentation is a preference, not a filter")
		gomega.Expect(pod.Spec.NodeName).ToNot(gomega.BeEmpty())
	})

	ginkgo.It("should split a burst of whole-cache claims across nodes", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("ccx-burst")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)
		if !ccxSchedulerPresent(ctx, fxt) {
			ginkgo.Skip("the CCXAlign scheduler is not deployed")
		}

		// The smallest whole cache free anywhere, so every node can take one.
		size := 0
		for _, node := range nodes {
			fit, ok := getFitAnnotation(ctx, fxt.K8SClientset, node)
			gomega.Expect(ok).To(gomega.BeTrue())
			largest := 0
			for _, numa := range fit.NUMA {
				if free := largestOf(numa.FreeCPUs); free > largest {
					largest = free
				}
			}
			if largest == 0 {
				ginkgo.Skip(fmt.Sprintf("node %s has no whole cache free", node))
			}
			if size == 0 || largest < size {
				size = largest
			}
		}

		// The precondition, which has to be built rather than assumed: every
		// node down to exactly one cache that can still hold the claim. A node
		// with two of them would take both pods for good reasons, and the
		// second pod scoring against a view that still counts the first would
		// look identical to it scoring correctly.
		ginkgo.By(fmt.Sprintf("leaving every node exactly one cache able to hold %d CPUs", size))
		for _, node := range nodes {
			leaveOneCacheFor(ctx, fxt, dracpuTesterImage, node, cfgValues, size, "ccx-burst-fill-"+node)
			gomega.Expect(cachesThatCanHold(ctx, fxt.K8SClientset, node, size)).To(gomega.Equal(1),
				"could not reduce %s to a single cache able to hold %d CPUs", node, size)
		}

		ginkgo.By(fmt.Sprintf("creating two %d-CPU claims at once", size))
		createClaimTemplate(ctx, fxt, "cpu-claim-burst", makeResourceClaimSpec(size, cfgValues.CPUDeviceMode == "grouped"))
		var pods []*v1.Pod
		for i := range 2 {
			pod := makeUnpinnedClaimPodOn(fxt.Namespace.Name, dracpuTesterImage, "cpu-claim-burst", ccxSchedulerName)
			pod.Name = fmt.Sprintf("ccx-burst-%d", i)
			pod.GenerateName = ""
			created, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			pods = append(pods, created)
		}

		landed := map[string]string{}
		for _, pod := range pods {
			gomega.Eventually(func() string {
				current, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					return ""
				}
				return current.Spec.NodeName
			}).WithTimeout(3 * time.Minute).WithPolling(2 * time.Second).ShouldNot(gomega.BeEmpty())
			current, err := fxt.K8SClientset.CoreV1().Pods(fxt.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			landed[pod.Name] = current.Spec.NodeName
		}

		gomega.Expect(landed[pods[0].Name]).ToNot(gomega.Equal(landed[pods[1].Name]),
			"both whole-cache claims landed on %s: the second was scored against a view the first had already spent", landed[pods[0].Name])
	})
})
