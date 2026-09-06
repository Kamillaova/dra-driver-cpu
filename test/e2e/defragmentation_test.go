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
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/cpuset"
)

const placementsPath = "/placements"

// placementsReport mirrors the driver's /placements payload. Only the fields the
// tests read are declared.
type placementsReport struct {
	DefragEnabled bool   `json:"defragEnabled"`
	SharedCPUs    string `json:"sharedCPUs"`
	Claims        []struct {
		ClaimUID   string `json:"claimUID"`
		CPUs       string `json:"cpus"`
		MovingFrom string `json:"movingFrom"`
	} `json:"claims"`
	NUMANodes []struct {
		NUMANodeID               int    `json:"numaNodeID"`
		FreeCPUs                 string `json:"freeCPUs"`
		ExcessUncoreCaches       int    `json:"excessUncoreCaches"`
		LargestAlignableFreeCPUs int    `json:"largestAlignableFreeCPUs"`
		Caches                   []struct {
			CacheID  int    `json:"cacheID"`
			CPUs     string `json:"cpus"`
			FreeCPUs string `json:"freeCPUs"`
		} `json:"caches"`
		Plan *struct {
			Moves []struct {
				ClaimUID string `json:"claimUID"`
				From     string `json:"from"`
				To       string `json:"to"`
			} `json:"moves"`
			CurrentCost int    `json:"currentCost"`
			IdealCost   int    `json:"idealCost"`
			Blocked     int    `json:"blocked"`
			Reason      string `json:"reason"`
		} `json:"plan"`
	} `json:"numaNodes"`
	Unmeasurable []struct {
		NUMANodeID int    `json:"numaNodeID"`
		Reason     string `json:"reason"`
	} `json:"unmeasurableNUMANodes"`
}

// totalExcess is the node's alignment cost across all its NUMA nodes.
func (r placementsReport) totalExcess() int {
	total := 0
	for _, node := range r.NUMANodes {
		total += node.ExcessUncoreCaches
	}
	return total
}

// plannedMoves is how many moves a dry run says a pass would make.
func (r placementsReport) plannedMoves() int {
	total := 0
	for _, node := range r.NUMANodes {
		if node.Plan != nil {
			total += len(node.Plan.Moves)
		}
	}
	return total
}

// freePerCacheOn is the free CPU count of every cache on one NUMA node,
// ascending. Fragmentation scenarios are built per NUMA node, because a move
// never crosses one: free space elsewhere cannot repair a claim here.
func (r placementsReport) freePerCacheOn(numaID int) []int {
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
	sort.Ints(free)
	return free
}

// numaWithMostCaches is where a fragmentation scenario has the most room to
// play out.
func (r placementsReport) numaWithMostCaches() int {
	best, caches := 0, -1
	for _, node := range r.NUMANodes {
		if len(node.Caches) > caches {
			best, caches = node.NUMANodeID, len(node.Caches)
		}
	}
	return best
}

// fillCachesDownTo fills every cache of one NUMA node down to leave free CPUs.
// Each filler is sized for the cache the deployed placement policy will land
// it in -- pack best-fits the fullest cache that fits, spread takes the
// least-tenanted one -- because a filler sized for one cache and landed in
// another leaves a hole big enough for the victim to fit whole, and the
// scenario never fragments.
func fillCachesDownTo(ctx context.Context, fxt *fixture.Fixture, image, nodeName string, cfg driverConfigValues, numaID, leave int, prefix string) []*v1.Pod {
	ginkgo.GinkgoHelper()
	var fillers []*v1.Pod
	for i := 0; i < 64; i++ {
		report, err := getPlacements(ctx, fxt.K8SClientset, nodeName, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		free := report.freePerCacheOn(numaID)
		size := 0
		if cfg.CachePlacementPolicy == "spread" {
			if len(free) > 0 && free[len(free)-1] > leave {
				size = free[len(free)-1] - leave
			}
		} else {
			for _, cacheFree := range free {
				if cacheFree > leave {
					size = cacheFree - leave
					break
				}
			}
		}
		if size <= 0 {
			break
		}
		pod, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, image, nodeName,
			claimSpecWithSelector(size, numaCEL(cfg, numaID)), fmt.Sprintf("%s-%d", prefix, i))
		if err != nil {
			fxt.Log.Info("stopped filling", "placed", len(fillers), "reason", err.Error())
			break
		}
		fillers = append(fillers, pod)
	}
	return fillers
}

// caches is how many uncore caches the driver can see across the node.
func (r placementsReport) caches() int {
	total := 0
	for _, node := range r.NUMANodes {
		total += len(node.Caches)
	}
	return total
}

func (r placementsReport) claimCPUs(claimUID string) (cpuset.CPUSet, bool) {
	for _, claim := range r.Claims {
		if claim.ClaimUID != claimUID {
			continue
		}
		cpus, err := cpuset.Parse(claim.CPUs)
		if err != nil {
			return cpuset.New(), false
		}
		return cpus, true
	}
	return cpuset.New(), false
}

func placementsURL(podIP string, dryRun bool) string {
	url := fmt.Sprintf("http://%s:%d%s", podIP, driverHTTPPort, placementsPath)
	if dryRun {
		url += "?dryrun=1"
	}
	return url
}

func getPlacements(ctx context.Context, cs kubernetes.Interface, nodeName string, dryRun bool) (placementsReport, error) {
	var report placementsReport

	driverPod, err := e2epod.GetDRACPUPod(ctx, cs, nodeName)
	if err != nil {
		return report, err
	}
	podIP, err := waitForPodIP(ctx, cs, driverPod.Name)
	if err != nil {
		return report, err
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(placementsURL(podIP, dryRun)) //nolint:noctx // httpClient.Timeout covers this
	if err != nil {
		return report, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return report, err
	}
	if resp.StatusCode != http.StatusOK {
		return report, fmt.Errorf("GET %s returned %d: %s", placementsURL(podIP, dryRun), resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, &report); err != nil {
		return report, fmt.Errorf("cannot parse placements payload %q: %w", string(body), err)
	}
	return report, nil
}

var _ = ginkgo.Describe("CPU Defragmentation", ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		targetNode        *v1.Node
		dracpuTesterImage string
		cfgValues         driverConfigValues
		baseline          placementsReport
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")

		targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, 1*time.Minute, rootFxt.Log)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name)

		cfgValues = getDriverConfig(ctx, rootFxt.K8SClientset)
		if !cfgValues.DefragEnabled {
			ginkgo.Skip("defragmentation is not enabled in the driver configuration; set DRACPU_E2E_DEFRAG=true when creating the cluster")
		}

		baseline, err = getPlacements(ctx, rootFxt.K8SClientset, targetNode.Name, true)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot read placements")
		rootFxt.Log.Info("baseline placements", "caches", baseline.caches(),
			"unmeasurableNodes", len(baseline.Unmeasurable), "sharedCPUs", baseline.SharedCPUs)

		// On a node with one uncore cache per NUMA node there is no spread to
		// recover, and on one that reports no cache IDs at all there is nothing
		// to reason about. Both are correct behaviours, not failures.
		if baseline.caches() < 2 {
			ginkgo.Skip(fmt.Sprintf("defragmentation needs at least two uncore caches the driver can see; this node reports %d measurable and %d unmeasurable NUMA nodes",
				baseline.caches(), len(baseline.Unmeasurable)))
		}
	})

	ginkgo.It("should report a consistent view of where claims are placed", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("placements")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("creating a pod with an exclusive claim")
		pod, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-placements")
		alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
		gomega.Expect(alloc.CPUAssigned).To(cpusetmatchers.HaveSize(2))

		ginkgo.By("checking the driver reports those very CPUs")
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(report.DefragEnabled).To(gomega.BeTrue())

			held := cpuset.New()
			for _, claim := range report.Claims {
				cpus, err := cpuset.Parse(claim.CPUs)
				g.Expect(err).ToNot(gomega.HaveOccurred())
				held = held.Union(cpus)
			}
			g.Expect(alloc.CPUAssigned.IsSubsetOf(held)).To(gomega.BeTrue(),
				"the pod runs on %s, which the driver does not report as claimed (%s)", alloc.CPUAssigned.String(), held.String())

			shared, err := cpuset.Parse(report.SharedCPUs)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(held.Intersection(shared).IsEmpty()).To(gomega.BeTrue(),
				"claimed CPUs %s overlap the shared pool %s", held.String(), shared.String())
		}, pollTimeoutRule, pollIntervalRule).Should(gomega.Succeed())
	})

	ginkgo.It("should consolidate fragmented claims without restarting their containers", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("defrag")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		// Fragmenting a node takes arithmetic, not just a lot of claims. No single
		// cache may have room for the next claim while the node as a whole still
		// does, and that depends on how big the caches are, so the sizes come from
		// the node's own geometry.
		//
		// Everything is counted in allocation steps rather than CPUs, because the
		// driver may publish a capacity request policy that rounds a request up to
		// whole cores; a size that is not a multiple of the step is not the size
		// the claim will get. Two steps are left free per cache and the split claim
		// asks for three: no cache can hold it, and one step is still over
		// afterwards. That last step matters -- a claim that would empty the shared
		// pool while other containers are running is refused, since NRI cannot
		// express an empty cpuset, so a tighter fit would be rejected rather than
		// split.
		step := allocationStep(ctx, fxt.K8SClientset, targetNode.Name)
		victimCPUs := 3 * step
		leaveFreePerCache := 2 * step
		// Per NUMA node, not per machine: a move never crosses a NUMA node, so
		// free space on another one cannot repair the victim, and a scenario
		// spanning both would ask the driver for a repair it must not make.
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		fxt.Log.Info("cache geometry", "numaNode", fragNUMA, "freePerCache", free, "allocationStep", step)
		if len(free) < 2 || free[0] < 3*step {
			ginkgo.Skip(fmt.Sprintf("fragmenting needs at least two caches with %d or more free CPUs each, got %v",
				3*step, free))
		}

		ginkgo.By(fmt.Sprintf("filling every cache of NUMA node %d down to two allocation steps free", fragNUMA))
		fillers := fillCachesDownTo(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, fragNUMA, leaveFreePerCache, "cpu-claim-filler")
		if len(fillers) < 2 {
			ginkgo.Skip(fmt.Sprintf("could only place %d of %d fillers", len(fillers), len(free)))
		}

		ginkgo.By("placing a claim that no single cache can hold")
		victim, victimUID, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			claimSpecWithSelector(victimCPUs, numaCEL(cfgValues, fragNUMA)), "cpu-claim-victim")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		before := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, victim)
		fxt.Log.Info("victim placement", "cpus", before.CPUAssigned.String())

		// The whole point of the test. If the arrangement did not actually
		// fragment anything there is nothing to consolidate, and passing here
		// would assert only that an already-packed node stays packed.
		ginkgo.By("verifying the node really is fragmented")
		fragmented, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if fragmented.totalExcess() == 0 {
			ginkgo.Skip(fmt.Sprintf("the node did not fragment: %+v", fragmented.NUMANodes))
		}
		gomega.Expect(before.CPUAssigned.Size()).To(gomega.Equal(victimCPUs))

		ginkgo.By("releasing a filler, which gives the split claim somewhere to go")
		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, fillers[0])).To(gomega.Succeed())

		ginkgo.By("waiting for the node to settle")
		var settled placementsReport
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, true)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			// Settled means a pass would do nothing more: the fixed point the
			// planner promises, where no move it can make would improve the node.
			g.Expect(report.plannedMoves()).To(gomega.BeZero(), "a pass still wants to move claims: %+v", report.NUMANodes)
			settled = report
		}, 3*time.Minute, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("verifying the claim was actually moved and is no longer split")
		gomega.Expect(settled.totalExcess()).To(gomega.BeZero(),
			"claims still span more caches than their sizes require: %+v", settled.NUMANodes)
		after, ok := settled.claimCPUs(victimUID)
		gomega.Expect(ok).To(gomega.BeTrue(), "the victim claim vanished: %+v", settled.Claims)
		gomega.Expect(after).ToNot(cpusetmatchers.Equal(before.CPUAssigned),
			"the claim was never moved, so nothing was consolidated")
		gomega.Expect(after).To(cpusetmatchers.HaveSize(before.CPUAssigned.Size()),
			"a move must not change how many CPUs a claim has")

		ginkgo.By("verifying the container kept running throughout")
		reread, err := fxt.K8SClientset.CoreV1().Pods(victim.Namespace).Get(ctx, victim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses).ToNot(gomega.BeEmpty())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero(),
			"a move must not restart the container")

		live := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread)
		gomega.Expect(live.CPUAssigned).To(cpusetmatchers.Equal(after),
			"the container is not on the CPUs the driver says its claim holds")
		gomega.Expect(live.CPUAffinity.Equals(live.CPUAssigned)).To(gomega.BeTrue(),
			"the kernel affinity %s does not match the cgroup cpuset %s", live.CPUAffinity.String(), live.CPUAssigned.String())

		ginkgo.By("verifying the surviving claims never share a CPU")
		held := cpuset.New()
		for _, claim := range settled.Claims {
			cpus, err := cpuset.Parse(claim.CPUs)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(held.Intersection(cpus).IsEmpty()).To(gomega.BeTrue(),
				"claim %s on %s overlaps another claim", claim.ClaimUID, cpus.String())
			held = held.Union(cpus)
		}
	})

	ginkgo.It("should keep a moved claim's cpuset across an in-place pod resize", func(ctx context.Context) {
		// KEP-1287 resizes reach the runtime as UpdateContainerResources, the
		// same CRI call a move uses. The failure this guards against is the
		// runtime rebuilding the container's resources from its create-time
		// state: a resize after a move would then drag the container back onto
		// CPUs its claim no longer holds, silently, with nothing restarted.
		fxt := rootFxt.WithPrefix("defrag-resize")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		step := allocationStep(ctx, fxt.K8SClientset, targetNode.Name)
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		if len(free) < 2 || free[0] < 3*step {
			ginkgo.Skip(fmt.Sprintf("needs at least two caches with %d or more free CPUs each, got %v", 3*step, free))
		}

		ginkgo.By(fmt.Sprintf("filling every cache of NUMA node %d down to two allocation steps free", fragNUMA))
		fillers := fillCachesDownTo(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, fragNUMA, 2*step, "cpu-claim-rs-filler")
		if len(fillers) < 2 {
			ginkgo.Skip(fmt.Sprintf("could only place %d of %d fillers", len(fillers), len(free)))
		}

		// The victim carries a real cpu request, because that is what a resize
		// changes; the claim pods elsewhere in this suite deliberately have none.
		ginkgo.By("placing a resizable claim that no single cache can hold")
		claimTemplate := resourcev1.ResourceClaimTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "cpu-claim-resize"},
			Spec: resourcev1.ResourceClaimTemplateSpec{
				Spec: claimSpecWithSelector(3*step, numaCEL(cfgValues, fragNUMA)),
			},
		}
		created, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, &claimTemplate, metav1.CreateOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		small := v1.ResourceList{v1.ResourceCPU: resource.MustParse("100m"), v1.ResourceMemory: resource.MustParse("64Mi")}
		victim := makeTesterPodWithExclusiveCPUClaimResources(fxt.Namespace.Name, dracpuTesterImage, created.Name, targetNode.Name, small, small)
		victim, err = e2epod.CreateSync(ctx, fxt.K8SClientset, victim)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		before := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, victim)

		report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if report.totalExcess() == 0 {
			ginkgo.Skip(fmt.Sprintf("the node did not fragment: %+v", report.NUMANodes))
		}

		ginkgo.By("releasing a filler and waiting for the claim to be moved")
		gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, fillers[len(fillers)-1])).To(gomega.Succeed())
		var moved cpuset.CPUSet
		gomega.Eventually(func(g gomega.Gomega) {
			alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, victim)
			g.Expect(alloc.CPUAssigned).ToNot(cpusetmatchers.Equal(before.CPUAssigned), "the claim has not been moved yet")
			moved = alloc.CPUAssigned
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("resizing the pod in place")
		limitBefore := currentCPULimit(ctx, fxt.K8SClientset, victim)
		patch := []byte(`{"spec":{"containers":[{"name":"tester-container-1","resources":{"requests":{"cpu":"200m","memory":"64Mi"},"limits":{"cpu":"200m","memory":"64Mi"}}}]}}`)
		_, err = fxt.K8SClientset.CoreV1().Pods(victim.Namespace).Patch(ctx, victim.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "resize")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		ginkgo.By("verifying the resize applied without touching the moved cpuset")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(currentCPULimit(ctx, fxt.K8SClientset, victim)).ToNot(gomega.Equal(limitBefore), "the resize has not been actuated yet")
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())

		reread, err := fxt.K8SClientset.CoreV1().Pods(victim.Namespace).Get(ctx, victim.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero(), "an in-place resize must not restart the container")
		gomega.Consistently(func(g gomega.Gomega) {
			alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread)
			g.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(moved),
				"the resize dragged the container off the CPUs its claim holds")
			g.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(moved))
		}, 20*time.Second, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should preserve exclusivity through claim churn, moves, and a driver restart", func(ctx context.Context) {
		// The scenario the feature exists for is not one tidy move but months of
		// claims coming and going. This compresses that into minutes: a
		// deterministic fragmentation first, then seeded-random churn, a driver
		// restart in the middle, and a full drain at the end -- with the
		// exclusivity invariants checked after every step, not only at the end.
		fxt := rootFxt.WithPrefix("defrag-soak")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		step := allocationStep(ctx, fxt.K8SClientset, targetNode.Name)
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		if len(free) < 2 || free[0] < 3*step {
			ginkgo.Skip(fmt.Sprintf("needs at least two caches with %d or more free CPUs each, got %v", 3*step, free))
		}
		seed := ginkgo.GinkgoRandomSeed()
		rng := rand.New(rand.NewPCG(uint64(seed), 0)) //nolint:gosec // reproducible via --ginkgo.seed
		fxt.Log.Info("soak parameters", "seed", seed, "step", step, "freePerCache", free)

		ginkgo.By("keeping one shared container running throughout")
		sharedPod := mustCreateBestEffortPod(ctx, fxt, targetNode.Name, dracpuTesterImage)

		type trackedClaim struct {
			pod  *v1.Pod
			size int
		}
		claims := map[string]*trackedClaim{}
		nextID := 0
		tryCreate := func(sizeSteps int, cel string) bool {
			name := fmt.Sprintf("cpu-claim-soak-%d", nextID)
			nextID++
			pod, uid, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
				claimSpecWithSelector(sizeSteps*step, cel), name)
			if err != nil {
				// A refused create is a legitimate outcome of churn: the pool may
				// be too full, or too fragmented for the size. The soak's job is
				// that whatever happens, no invariant breaks.
				fxt.Log.Info("create refused", "cpus", sizeSteps*step, "reason", err.Error())
				return false
			}
			claims[uid] = &trackedClaim{pod: pod, size: sizeSteps * step}
			return true
		}

		settle := func() {
			gomega.Eventually(func(g gomega.Gomega) {
				report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, true)
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(report.plannedMoves()).To(gomega.BeZero())
			}, 3*time.Minute, 5*time.Second).Should(gomega.Succeed())
		}
		verifyInvariants := func() {
			ginkgo.GinkgoHelper()
			gomega.Eventually(func(g gomega.Gomega) {
				report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
				g.Expect(err).ToNot(gomega.HaveOccurred())

				union := cpuset.New()
				for uid, tracked := range claims {
					cpus, ok := report.claimCPUs(uid)
					g.Expect(ok).To(gomega.BeTrue(), "claim %s vanished from the driver", uid)
					g.Expect(cpus.Size()).To(gomega.Equal(tracked.size), "claim %s changed size", uid)
					g.Expect(cpus.Contains(0)).To(gomega.BeFalse(), "claim %s took the reserved CPU", uid)
					g.Expect(union.Intersection(cpus).IsEmpty()).To(gomega.BeTrue(),
						"claim %s on %s overlaps another claim", uid, cpus.String())
					union = union.Union(cpus)
				}
				shared, err := cpuset.Parse(report.SharedCPUs)
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(union.Intersection(shared).IsEmpty()).To(gomega.BeTrue(),
					"claimed CPUs leaked into the shared pool")

				for uid, tracked := range claims {
					cpus, _ := report.claimCPUs(uid)
					alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, tracked.pod)
					g.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(cpus),
						"container of claim %s is not on its claim's CPUs", uid)
					g.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(cpus))
				}
				sharedAlloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod)
				g.Expect(sharedAlloc.CPUAssigned.Intersection(union).IsEmpty()).To(gomega.BeTrue(),
					"the shared container holds a claimed CPU")
			}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())
		}

		ginkgo.By("phase 1: deterministic fragmentation and one guaranteed consolidation")
		for _, cacheFree := range free {
			if !tryCreate(cacheFree/step-2, numaCEL(cfgValues, fragNUMA)) {
				break
			}
		}
		fragmented := false
		if tryCreate(3, numaCEL(cfgValues, fragNUMA)) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			fragmented = report.totalExcess() > 0
		}
		settle()
		verifyInvariants()

		ginkgo.By("phase 2: seeded-random churn")
		for round := range 5 {
			uids := make([]string, 0, len(claims))
			for uid := range claims {
				uids = append(uids, uid)
			}
			sort.Strings(uids)
			if len(uids) > 1 && rng.IntN(2) == 0 {
				victim := uids[rng.IntN(len(uids))]
				gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, claims[victim].pod)).To(gomega.Succeed())
				delete(claims, victim)
			}
			tryCreate(1+rng.IntN(3), "")
			fxt.Log.Info("churn round complete", "round", round, "liveClaims", len(claims))
			settle()
			verifyInvariants()
		}

		if fragmented {
			ginkgo.By("verifying at least one move was committed while fragmented")
			moves := defragMovesCommitted(ctx, fxt.K8SClientset, targetNode.Name)
			gomega.Expect(moves).To(gomega.BeNumerically(">=", 1),
				"the node fragmented and settled, yet no move was ever committed")
		}

		ginkgo.By("phase 3: restarting the driver under load")
		byClaim := map[string]cpuset.CPUSet{}
		report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		for uid := range claims {
			cpus, ok := report.claimCPUs(uid)
			gomega.Expect(ok).To(gomega.BeTrue())
			byClaim[uid] = cpus
		}
		restartDriverOnNode(ctx, rootFxt, targetNode.Name)

		ginkgo.By("verifying the restart moved nothing")
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			for uid, want := range byClaim {
				cpus, ok := report.claimCPUs(uid)
				g.Expect(ok).To(gomega.BeTrue(), "claim %s was not adopted after the restart", uid)
				g.Expect(cpus).To(cpusetmatchers.Equal(want), "the restart relocated claim %s", uid)
			}
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())
		verifyInvariants()

		ginkgo.By("phase 4: churn continues after the restart")
		tryCreate(1+rng.IntN(3), "")
		settle()
		verifyInvariants()

		ginkgo.By("phase 5: draining every claim returns the node to a clean state")
		for uid, tracked := range claims {
			gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, tracked.pod)).To(gomega.Succeed())
			delete(claims, uid)
		}
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(report.Claims).To(gomega.BeEmpty(), "claims outlived their pods: %+v", report.Claims)
			g.Expect(report.totalExcess()).To(gomega.BeZero())
			if getDriverConfig(ctx, fxt.K8SClientset).reconcilesSharedOnUnprepare() {
				shared, err := cpuset.Parse(report.SharedCPUs)
				g.Expect(err).ToNot(gomega.HaveOccurred())
				alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, sharedPod)
				g.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(shared),
					"the shared container was not widened back onto the drained pool")
			}
		}, 2*time.Minute, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should keep a container on the union of its claims when one of them moves", func(ctx context.Context) {
		// A container may hold several claims, and a move touches one of them.
		// The update that carries the move must pin the container to the union of
		// everything it holds: pinning only the moved claim would take the other
		// claim's CPUs away from a running workload.
		fxt := rootFxt.WithPrefix("defrag-union")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		step := allocationStep(ctx, fxt.K8SClientset, targetNode.Name)
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		if len(free) < 2 || free[0] < 4*step {
			ginkgo.Skip(fmt.Sprintf("needs at least two caches with %d or more free CPUs each, got %v", 4*step, free))
		}

		ginkgo.By(fmt.Sprintf("filling every cache of NUMA node %d down to two allocation steps free", fragNUMA))
		fillers := fillCachesDownTo(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, fragNUMA, 2*step, "cpu-claim-un-filler")
		if len(fillers) < 2 {
			ginkgo.Skip(fmt.Sprintf("could only place %d of %d fillers", len(fillers), len(free)))
		}

		ginkgo.By("placing one pod holding a small claim and a straddling claim")
		smallSize, bigSize := 1*step, 3*step
		for name, cpus := range map[string]int{"cpu-claim-un-small": smallSize, "cpu-claim-un-big": bigSize} {
			template := resourcev1.ResourceClaimTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Spec: resourcev1.ResourceClaimTemplateSpec{
					Spec: claimSpecWithSelector(cpus, numaCEL(cfgValues, fragNUMA)),
				},
			}
			_, err := fxt.K8SClientset.ResourceV1().ResourceClaimTemplates(fxt.Namespace.Name).Create(ctx, &template, metav1.CreateOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
		}
		pod := makeTesterPodWithTwoClaims(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, "cpu-claim-un-small", "cpu-claim-un-big")
		pod, err := e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())

		smallUID, bigUID := claimUIDsBySize(ctx, fxt, pod, smallSize, bigSize)
		report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if report.totalExcess() == 0 {
			ginkgo.Skip(fmt.Sprintf("the big claim did not straddle: %+v", report.NUMANodes))
		}
		smallBefore, _ := report.claimCPUs(smallUID)

		ginkgo.By("releasing the fillers so the straddling claim can move")
		for _, filler := range fillers {
			gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, filler)).To(gomega.Succeed())
		}

		ginkgo.By("verifying the container ends on the union, the small claim untouched")
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			big, ok := report.claimCPUs(bigUID)
			g.Expect(ok).To(gomega.BeTrue())
			g.Expect(spreadOf(report, big)).To(gomega.Equal(1), "the big claim is still split: %s", big.String())
			small, ok := report.claimCPUs(smallUID)
			g.Expect(ok).To(gomega.BeTrue())
			g.Expect(small).To(cpusetmatchers.Equal(smallBefore), "the small claim had no reason to move")

			alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)
			g.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(small.Union(big)),
				"the container must hold the union of both its claims")
			g.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(small.Union(big)))
		}, 3*time.Minute, 5*time.Second).Should(gomega.Succeed())

		reread, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero())
	})

	ginkgo.It("should consolidate lone one-core claims to admit a claim of two whole caches", func(ctx context.Context) {
		// The confetti worst case: churn leaves one small claim marooned in
		// every cache, each individually placed as well as it can be, so a
		// planner that pins "already-optimal" claims would never free a cache
		// again. The scheduler then admits a two-cache claim against the
		// device's scalar capacity -- correctly, the CPUs exist -- and only a
		// from-scratch repack that relocates bystanders can align it.
		fxt := rootFxt.WithPrefix("defrag-confetti")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		step := allocationStep(ctx, fxt.K8SClientset, targetNode.Name)
		fragNUMA := baseline.numaWithMostCaches()
		free := baseline.freePerCacheOn(fragNUMA)
		if len(free) < 4 {
			ginkgo.Skip(fmt.Sprintf("the confetti scenario needs at least four caches on one NUMA node, got %d", len(free)))
		}
		cacheSize := free[len(free)-1]
		bigSize := 2 * cacheSize
		if free[len(free)-2] != cacheSize {
			ginkgo.Skip(fmt.Sprintf("needs two caches of %d allocatable CPUs for the big claim, free: %v", cacheSize, free))
		}

		minNonzeroFree := func(report placementsReport) int {
			smallest := 0
			for _, node := range report.NUMANodes {
				if node.NUMANodeID != fragNUMA {
					continue
				}
				for _, cache := range node.Caches {
					cpus, err := cpuset.Parse(cache.FreeCPUs)
					if err != nil || cpus.Size() == 0 {
						continue
					}
					if smallest == 0 || cpus.Size() < smallest {
						smallest = cpus.Size()
					}
				}
			}
			return smallest
		}
		cachesTouched := func(report placementsReport, cpus cpuset.CPUSet) []int {
			var touched []int
			for _, node := range report.NUMANodes {
				for _, cache := range node.Caches {
					inCache, err := cpuset.Parse(cache.CPUs)
					if err != nil {
						continue
					}
					if !cpus.Intersection(inCache).IsEmpty() {
						touched = append(touched, cache.CacheID)
					}
				}
			}
			return touched
		}

		var smallUIDs []string
		if cfgValues.CachePlacementPolicy == "spread" {
			// Under spread the confetti is not an accident of churn but the
			// allocator's own preference: consecutive smalls land one per cache.
			ginkgo.By("placing one one-core claim per cache, as spread does naturally")
			for i := range free {
				_, uid, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
					claimSpecWithSelector(step, numaCEL(cfgValues, fragNUMA)), fmt.Sprintf("cpu-claim-cf-small-%d", i))
				if err != nil {
					break
				}
				smallUIDs = append(smallUIDs, uid)
			}
		} else {
			ginkgo.By("marooning one one-core claim in every cache: a small, then a filler sealing its cache")
			var fillers []*v1.Pod
			for i := range free {
				report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				hole := minNonzeroFree(report)
				if hole < 2*step {
					break
				}
				_, uid, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
					claimSpecWithSelector(step, numaCEL(cfgValues, fragNUMA)), fmt.Sprintf("cpu-claim-cf-small-%d", i))
				if err != nil {
					break
				}
				smallUIDs = append(smallUIDs, uid)
				filler, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
					claimSpecWithSelector(hole-step, numaCEL(cfgValues, fragNUMA)), fmt.Sprintf("cpu-claim-cf-filler-%d", i))
				if err != nil {
					break
				}
				fillers = append(fillers, filler)
			}
			defer func() {
				for _, filler := range fillers {
					_ = e2epod.DeleteSync(ctx, fxt.K8SClientset, filler)
				}
			}()
			if len(fillers) < len(smallUIDs) {
				ginkgo.Skip(fmt.Sprintf("could not maroon enough smalls (%d smalls, %d fillers)", len(smallUIDs), len(fillers)))
			}
			ginkgo.By("releasing the fillers, leaving only the marooned smalls")
			for _, filler := range fillers {
				gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, filler)).To(gomega.Succeed())
			}
			fillers = nil
		}
		if len(smallUIDs) < 3 {
			ginkgo.Skip(fmt.Sprintf("could not place enough smalls (%d)", len(smallUIDs)))
		}

		ginkgo.By("verifying the confetti actually formed: every small alone in a distinct cache")
		shape, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		seen := map[int]bool{}
		scattered := true
		for _, uid := range smallUIDs {
			cpus, ok := shape.claimCPUs(uid)
			gomega.Expect(ok).To(gomega.BeTrue(), "small claim %s is not reported", uid)
			touched := cachesTouched(shape, cpus)
			if len(touched) != 1 || seen[touched[0]] {
				scattered = false
				break
			}
			seen[touched[0]] = true
		}
		if !scattered {
			gomega.Expect(cfgValues.CachePlacementPolicy).ToNot(gomega.Equal("spread"),
				"under spread, one claim per cache is the allocator's own promise, not a scenario to construct")
			ginkgo.Skip("the allocator did not scatter the smalls one per cache; the scenario cannot form on this geometry")
		}

		movesBefore := defragMovesCommitted(ctx, fxt.K8SClientset, targetNode.Name)
		bystanders := map[string]cpuset.CPUSet{}
		for _, uid := range smallUIDs {
			cpus, ok := shape.claimCPUs(uid)
			gomega.Expect(ok).To(gomega.BeTrue())
			bystanders[uid] = cpus
		}

		ginkgo.By(fmt.Sprintf("requesting %d CPUs: two whole caches, while every cache hosts a small", bigSize))
		bigPod, bigUID, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
			claimSpecWithSelector(bigSize, numaCEL(cfgValues, fragNUMA)), "cpu-claim-cf-big")
		gomega.Expect(err).ToNot(gomega.HaveOccurred(),
			"the scheduler must admit the claim: the CPUs exist, only their shape is wrong")

		fragmented, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		bigBefore, ok := fragmented.claimCPUs(bigUID)
		gomega.Expect(ok).To(gomega.BeTrue())
		spreadBefore := spreadOf(fragmented, bigBefore)
		fxt.Log.Info("big claim landed", "cpus", bigBefore.String(), "spread", spreadBefore, "smalls", len(smallUIDs))
		if spreadBefore <= 2 {
			ginkgo.Skip(fmt.Sprintf("the big claim landed on %d caches; nothing to consolidate", spreadBefore))
		}

		ginkgo.By("waiting for defragmentation to relocate the bystanders and align the claim")
		var settled placementsReport
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, true)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(report.plannedMoves()).To(gomega.BeZero())
			g.Expect(report.totalExcess()).To(gomega.BeZero(),
				"claims still span more caches than their sizes require: %+v", report.NUMANodes)
			settled = report
		}, 4*time.Minute, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("verifying the big claim owns exactly two caches and every small survived intact")
		bigAfter, ok := settled.claimCPUs(bigUID)
		gomega.Expect(ok).To(gomega.BeTrue(), "the big claim vanished: %+v", settled.Claims)
		gomega.Expect(spreadOf(settled, bigAfter)).To(gomega.Equal(2), "the big claim is not on two whole caches: %s", bigAfter.String())
		gomega.Expect(bigAfter).To(cpusetmatchers.HaveSize(bigSize))
		union := bigAfter
		for _, uid := range smallUIDs {
			cpus, ok := settled.claimCPUs(uid)
			gomega.Expect(ok).To(gomega.BeTrue(), "small claim %s vanished", uid)
			gomega.Expect(cpus).To(cpusetmatchers.HaveSize(step), "small claim %s changed size", uid)
			gomega.Expect(spreadOf(settled, cpus)).To(gomega.Equal(1), "small claim %s ended split", uid)
			gomega.Expect(cpus).To(cpusetmatchers.HaveNoOverlapWith(union), "claim %s overlaps another claim", uid)
			union = union.Union(cpus)
		}

		// The alignment cannot come for free -- the big claim moved and the two
		// smalls in its way stepped aside -- but it must cost no more than that:
		// three claims touched, at most four commits (the straddler may take an
		// interim step, since it cannot land on CPUs a bystander still holds in
		// the same pass), and every other small keeps its exact CPUs.
		moves := defragMovesCommitted(ctx, fxt.K8SClientset, targetNode.Name) - movesBefore
		gomega.Expect(moves).To(gomega.BeNumerically(">=", 3),
			"aligning the big claim requires moving it plus at least the two smalls in its way")
		gomega.Expect(moves).To(gomega.BeNumerically("<=", 4),
			"the repair herded bystanders it did not need")
		movedSmalls := 0
		for uid, before := range bystanders {
			cpus, ok := settled.claimCPUs(uid)
			gomega.Expect(ok).To(gomega.BeTrue())
			if !cpus.Equals(before) {
				movedSmalls++
			}
		}
		gomega.Expect(movedSmalls).To(gomega.BeNumerically("<=", 2),
			"only the smalls inside the big claim's two target caches may move, %d did", movedSmalls)

		ginkgo.By("verifying the container followed its claim without a restart")
		reread, err := fxt.K8SClientset.CoreV1().Pods(bigPod.Namespace).Get(ctx, bigPod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero())
		want := bigAfter
		gomega.Eventually(func(g gomega.Gomega) {
			alloc := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread)
			g.Expect(alloc.CPUAssigned).To(cpusetmatchers.Equal(want))
			g.Expect(alloc.CPUAffinity).To(cpusetmatchers.Equal(want))
		}, 30*time.Second, 5*time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should recover a claim's placement when the driver restarts", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("defrag-restart")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		ginkgo.By("placing a claim")
		pod, claimUID := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-restart")
		before := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, pod)

		ginkgo.By("restarting the driver on the target node")
		restartDriverOnNode(ctx, rootFxt, targetNode.Name)

		ginkgo.By("verifying the driver rebuilt the same placement from the specs on disk")
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, false)
			g.Expect(err).ToNot(gomega.HaveOccurred())

			cpus, ok := report.claimCPUs(claimUID)
			g.Expect(ok).To(gomega.BeTrue(), "claim %s was not adopted after the restart: %+v", claimUID, report.Claims)
			g.Expect(cpus.Size()).To(gomega.Equal(before.CPUAssigned.Size()),
				"the claim's CPU count changed across the restart")

			shared, err := cpuset.Parse(report.SharedCPUs)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(cpus.Intersection(shared).IsEmpty()).To(gomega.BeTrue(),
				"the claim's CPUs %s were handed back to the shared pool %s", cpus.String(), shared.String())
		}, pollTimeoutRule, pollIntervalRule).Should(gomega.Succeed())

		ginkgo.By("verifying the container is still pinned and was not restarted")
		reread, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero())
		after := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread)
		gomega.Expect(after.CPUAssigned).To(cpusetmatchers.HaveSize(before.CPUAssigned.Size()))
	})
})

// restartDriverOnNode deletes the driver pod on a node and waits for the
// DaemonSet's replacement to be running, which is how a restart looks to the
// claims already on that node.
func restartDriverOnNode(ctx context.Context, fxt *fixture.Fixture, nodeName string) {
	ginkgo.GinkgoHelper()

	old, err := e2epod.GetDRACPUPod(ctx, fxt.K8SClientset, nodeName)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot find the driver pod on %s", nodeName)
	gomega.Expect(fxt.K8SClientset.CoreV1().Pods(old.Namespace).Delete(ctx, old.Name, metav1.DeleteOptions{})).To(gomega.Succeed())

	gomega.Eventually(func(g gomega.Gomega) {
		current, err := e2epod.GetDRACPUPod(ctx, fxt.K8SClientset, nodeName)
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(current.UID).ToNot(gomega.Equal(old.UID), "the driver pod has not been replaced yet")
		g.Expect(current.Status.Phase).To(gomega.Equal(v1.PodRunning))
		for _, condition := range current.Status.Conditions {
			if condition.Type == v1.PodReady {
				g.Expect(condition.Status).To(gomega.Equal(v1.ConditionTrue), "the new driver pod is not ready")
			}
		}
	}, pollTimeoutRule, pollIntervalRule).Should(gomega.Succeed(), "the driver did not come back on %s", nodeName)
}

// allocationStep is the CPU granularity the scheduler will round a capacity
// request to, taken from the request policy the driver publishes on its device.
// One means a request is taken literally; two means whole SMT cores.
func allocationStep(ctx context.Context, cs kubernetes.Interface, nodeName string) int {
	ginkgo.GinkgoHelper()
	slices, err := cs.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())

	for _, slice := range slices.Items {
		if slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
			continue
		}
		for _, device := range slice.Spec.Devices {
			capacity, ok := device.Capacity["dra.cpu/cpu"]
			if !ok || capacity.RequestPolicy == nil || capacity.RequestPolicy.ValidRange == nil ||
				capacity.RequestPolicy.ValidRange.Step == nil {
				continue
			}
			step := capacity.RequestPolicy.ValidRange.Step.Value()
			if step > 1 {
				return int(step)
			}
		}
	}
	return 1
}

// currentCPULimit is the cpu limit the kubelet reports as actuated for the
// pod's first container, which is how a resize's completion is observed.
func currentCPULimit(ctx context.Context, cs kubernetes.Interface, pod *v1.Pod) string {
	ginkgo.GinkgoHelper()
	reread, err := cs.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	statuses := reread.Status.ContainerStatuses
	if len(statuses) == 0 || statuses[0].Resources == nil {
		return ""
	}
	return statuses[0].Resources.Limits.Cpu().String()
}

// defragMovesCommitted reads the driver's committed-move counter for the node.
// The counter resets when the driver restarts, so read it before one.
func defragMovesCommitted(ctx context.Context, cs kubernetes.Interface, nodeName string) int {
	ginkgo.GinkgoHelper()
	driverPod, err := e2epod.GetDRACPUPod(ctx, cs, nodeName)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	podIP, err := waitForPodIP(ctx, cs, driverPod.Name)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	raw, err := getMetricsFromPodIP(podIP)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	m := regexp.MustCompile(`dra_cpu_defrag_moves_total\{result="success"\} ([0-9]+)`).FindStringSubmatch(raw)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	return n
}

// spreadOf counts the uncore caches a cpuset touches, using the geometry the
// report itself carries.
func spreadOf(report placementsReport, cpus cpuset.CPUSet) int {
	touched := 0
	for _, node := range report.NUMANodes {
		for _, cache := range node.Caches {
			inCache, err := cpuset.Parse(cache.CPUs)
			if err != nil {
				continue
			}
			if !cpus.Intersection(inCache).IsEmpty() {
				touched++
			}
		}
	}
	return touched
}

// makeTesterPodWithTwoClaims builds a pod whose single container consumes two
// separate CPU claims, which the driver must pin to their union.
func makeTesterPodWithTwoClaims(ns, image, nodeName, templateA, templateB string) *v1.Pod {
	memory := resource.MustParse("64Mi")
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "tester-pod-two-claims-", Namespace: ns},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name:    "tester-container-1",
				Image:   image,
				Command: []string{"/dracputester"},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceMemory: memory},
					Limits:   v1.ResourceList{v1.ResourceMemory: memory},
					Claims:   []v1.ResourceClaim{{Name: "cpu-a"}, {Name: "cpu-b"}},
				},
			}},
			ResourceClaims: []v1.PodResourceClaim{
				{Name: "cpu-a", ResourceClaimTemplateName: &templateA},
				{Name: "cpu-b", ResourceClaimTemplateName: &templateB},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	return e2epod.PinToNode(pod, nodeName)
}

// claimUIDsBySize resolves the pod's two generated claims to their UIDs by the
// capacity each one requested.
func claimUIDsBySize(ctx context.Context, fxt *fixture.Fixture, pod *v1.Pod, smallSize, bigSize int) (string, string) {
	ginkgo.GinkgoHelper()
	var smallUID, bigUID string
	gomega.Eventually(func(g gomega.Gomega) {
		list, err := fxt.K8SClientset.ResourceV1().ResourceClaims(fxt.Namespace.Name).List(ctx, metav1.ListOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		for _, claim := range list.Items {
			owned := false
			for _, ref := range claim.OwnerReferences {
				if ref.UID == pod.UID {
					owned = true
				}
			}
			if !owned {
				continue
			}
			quantity := claim.Spec.Devices.Requests[0].Exactly.Capacity.Requests["dra.cpu/cpu"]
			switch int(quantity.Value()) {
			case smallSize:
				smallUID = string(claim.UID)
			case bigSize:
				bigUID = string(claim.UID)
			}
		}
		g.Expect(smallUID).ToNot(gomega.BeEmpty(), "no claim of %d CPUs owned by the pod", smallSize)
		g.Expect(bigUID).ToNot(gomega.BeEmpty(), "no claim of %d CPUs owned by the pod", bigSize)
	}, time.Minute, 2*time.Second).Should(gomega.Succeed())
	return smallUID, bigUID
}
