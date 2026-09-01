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
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	cpusetmatchers "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/matchers/cpuset"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

		ginkgo.By(fmt.Sprintf("filling every cache of NUMA node %d down to two allocation steps free, smallest cache first", fragNUMA))
		var fillers []*v1.Pod
		for i, cacheFree := range free {
			pod, _, err := tryCreateClaimedTesterPodWithSpec(ctx, fxt, dracpuTesterImage, targetNode.Name,
				claimSpecWithSelector(cacheFree-leaveFreePerCache, numaCEL(cfgValues, fragNUMA)), fmt.Sprintf("cpu-claim-filler-%d", i))
			if err != nil {
				fxt.Log.Info("stopped filling", "placed", len(fillers), "reason", err.Error())
				break
			}
			fillers = append(fillers, pod)
		}
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

})

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
