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

		// Fill the node with small claims, then release every other one. What is
		// left is free space scattered across caches, which is the state a claim
		// arriving next has to be placed into and the state defragmentation exists
		// to repair.
		ginkgo.By("fragmenting the node with small claims")
		var pods []*v1.Pod
		for i := range 6 {
			pod, err := tryCreateClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 1,
				fmt.Sprintf("cpu-claim-frag-%d", i))
			if err != nil {
				// The node ran out of allocatable CPUs, which is the point of
				// filling it.
				fxt.Log.Info("stopped filling the node", "claims", len(pods), "reason", err.Error())
				break
			}
			pods = append(pods, pod)
		}
		if len(pods) < 4 {
			ginkgo.Skip(fmt.Sprintf("need at least 4 single-CPU claims to fragment this node, could only place %d", len(pods)))
		}

		ginkgo.By("releasing every other claim")
		for i := 0; i < len(pods); i += 2 {
			gomega.Expect(e2epod.DeleteSync(ctx, fxt.K8SClientset, pods[i])).To(gomega.Succeed())
		}

		ginkgo.By("placing a claim that needs more than one cache's worth of the remainder")
		survivor, _ := createClaimedTesterPod(ctx, fxt, dracpuTesterImage, targetNode.Name, cfgValues, 2, "cpu-claim-survivor")
		before := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, survivor)
		fxt.Log.Info("survivor placement", "cpus", before.CPUAssigned.String())

		ginkgo.By("waiting for the node to settle")
		var settled placementsReport
		gomega.Eventually(func(g gomega.Gomega) {
			report, err := getPlacements(ctx, fxt.K8SClientset, targetNode.Name, true)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			// Settled means a pass would do nothing more, which is the fixed point
			// the planner promises: no move it can make would improve the node.
			g.Expect(report.plannedMoves()).To(gomega.BeZero(), "a pass still wants to move claims: %+v", report.NUMANodes)
			settled = report
		}, 3*time.Minute, 5*time.Second).Should(gomega.Succeed())

		ginkgo.By("verifying no claim is left avoidably split")
		gomega.Expect(settled.totalExcess()).To(gomega.BeZero(),
			"claims still span more caches than their sizes require: %+v", settled.NUMANodes)

		ginkgo.By("verifying the container kept running throughout and still holds its CPUs")
		reread, err := fxt.K8SClientset.CoreV1().Pods(survivor.Namespace).Get(ctx, survivor.Name, metav1.GetOptions{})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(reread.Status.ContainerStatuses).ToNot(gomega.BeEmpty())
		gomega.Expect(reread.Status.ContainerStatuses[0].RestartCount).To(gomega.BeZero(),
			"a move must not restart the container")

		after := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, reread)
		gomega.Expect(after.CPUAssigned).To(cpusetmatchers.HaveSize(before.CPUAssigned.Size()),
			"a move must not change how many CPUs a claim has")
		gomega.Expect(after.CPUAffinity.Equals(after.CPUAssigned)).To(gomega.BeTrue(),
			"the kernel affinity %s does not match the cgroup cpuset %s", after.CPUAffinity.String(), after.CPUAssigned.String())

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
