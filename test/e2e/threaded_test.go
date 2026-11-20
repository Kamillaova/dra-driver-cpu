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
	"maps"
	"os"
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	e2eclient "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/client"
	"github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/fixture"
	e2enode "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/node"
	e2epod "github.com/kubernetes-sigs/dra-driver-cpu/test/pkg/pod"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/utils/cpuset"
)

const (
	// The runtime handler of the kind configurations whose containers get a
	// writable /sys/fs/cgroup, and the RuntimeClass the specs create for it,
	// unless DRACPU_E2E_RUNTIME_HANDLER names another.
	cgroupWritable = "cgroup-writable"
	// The user the specs' containers run as, which the driver hands the files
	// that move processes and threads to.
	threadedUser = 1000
	cgroupMount  = "/sys/fs/cgroup"
	cgroupLinks  = "plugins/dra.cpu/cgroup-links"
)

// What test/image/dracpucgroup prints.
type (
	cgroupOwner struct {
		UID int `json:"uid"`
		GID int `json:"gid"`
	}
	cgroupView struct {
		Name           string                 `json:"name"`
		Type           string                 `json:"type"`
		SubtreeControl string                 `json:"subtreeControl"`
		CPUs           string                 `json:"cpus"`
		CPUsEffective  string                 `json:"cpusEffective"`
		CPUMax         string                 `json:"cpuMax"`
		Weight         string                 `json:"weight"`
		Threads        []int                  `json:"threads"`
		Owners         map[string]cgroupOwner `json:"owners"`
	}
	threadedTree struct {
		Link       string       `json:"link"`
		Resolved   string       `json:"resolved"`
		InitCgroup string       `json:"initCgroup"`
		ExecCgroup string       `json:"execCgroup"`
		Root       cgroupView   `json:"root"`
		Groups     []cgroupView `json:"groups"`
	}
	workloadThread struct {
		TID    int    `json:"tid"`
		Cgroup string `json:"cgroup"`
		CPUs   string `json:"cpus"`
	}
	workloadReport struct {
		Pinned []workloadThread `json:"pinned"`
		Others []workloadThread `json:"others"`
		Errors []string         `json:"errors"`
	}
)

// expectedGroup is what a spec expects of one group: its CPUs, cpu.max and
// cpu.weight.
type expectedGroup struct {
	cpus   cpuset.CPUSet
	cpuMax string
	weight string
}

// threadedClaim is a claim of the specs and the group each of its requests'
// results lands in.
type threadedClaim struct {
	spec   resourcev1.ResourceClaimSpec
	groups map[string]string
}

// The threaded specs run on a node whose driver runs cgroupThreadedMode create
// or place, their pods under the RuntimeClass of threadedRuntimeHandler, whose
// handler the kind configurations declare, as an administrator would, and as
// non-root users.
// They read the groups from inside the container, through the link
// /var/run/dracpu/cgroup. Where the node's default partition gives exclusive
// CPUs and shares, a claim holds both; on a node without partitions, only
// exclusive CPUs.
var _ = ginkgo.Describe("Threaded cgroups", ginkgo.Label("threaded"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt           *fixture.Fixture
		restConfig        *rest.Config
		dracpuTesterImage string
		cfg               driverConfigValues
		targetNode        *v1.Node
		effective         driverConfigValues
		// shares reports whether targetNode's default partition gives shares
		// and, flagged, exclusive CPUs.
		shares              bool
		createdRuntimeClass bool
		runtimeHandler      = threadedRuntimeHandler()
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")

		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		restConfig, err = e2eclient.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create Kubernetes config")

		cfg = getDriverConfig(ctx, rootFxt.K8SClientset)
		if mode := string(cfg.CgroupThreadedMode); mode != cgroupfs.ThreadedModeCreate && mode != cgroupfs.ThreadedModePlace {
			ginkgo.Skip("the driver runs cgroupThreadedMode false; install it with create or place")
		}

		allowing, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel + "=" + allowingProfile})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		for i := range allowing.Items {
			node := &allowing.Items[i]
			if values := cfg.effectiveFor(node); values.partitionsUsed() && defaultEntry(values).AllowExclusive {
				targetNode, effective, shares = node, values, true
				break
			}
		}
		if targetNode == nil {
			targetNode, err = e2enode.PickWorker(ctx, rootFxt.K8SClientset, 5*time.Second, time.Minute, rootFxt.Log)
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			effective = cfg.effectiveFor(targetNode)
			if effective.partitionsUsed() {
				ginkgo.Skip("no node runs the profile " + allowingProfile + ", whose default partition allows exclusive requests, and node " + targetNode.Name + " gives shares only")
			}
		}
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name, "shares", shares, "mode", cfg.CgroupThreadedMode)
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		createdRuntimeClass = createRuntimeClass(ctx, rootFxt, runtimeHandler)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if createdRuntimeClass {
			deleteRuntimeClass(ctx, rootFxt, runtimeHandler)
		}
	})

	// baseClaim is the claim of the specs that need no particular shape: a VM's
	// vCPU, its capped housekeeping and an uncapped share, each in a group
	// named after it, where the node gives shares; elsewhere two exclusive
	// requests in one group and a third in its own.
	baseClaim := func(place string) threadedClaim {
		if shares {
			return vmClaim(place)
		}
		amount := "1"
		if cfg.CPUDeviceMode != "grouped" {
			amount = ""
		}
		spec := resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
			exactCPURequest("vcpu-a", driverName, amount),
			exactCPURequest("vcpu-b", driverName, amount),
			exactCPURequest("helper", driverName, amount),
		}}}
		spec.Devices.Config = []resourcev1.DeviceClaimConfiguration{threadedEntry(place,
			v1alpha1.ThreadedGroup{Name: "vcpu", Requests: []string{"vcpu-a", "vcpu-b"}},
			v1alpha1.ThreadedGroup{Name: "helper", Requests: []string{"helper"}},
		)}
		return threadedClaim{spec: spec, groups: map[string]string{"vcpu-a": "vcpu", "vcpu-b": "vcpu", "helper": "helper"}}
	}

	// baseGroups returns what the groups of baseClaim hold, from the claim's
	// own status.
	baseGroups := func(ctx context.Context, fxt *fixture.Fixture, claim *resourcev1.ResourceClaim) map[string]expectedGroup {
		ginkgo.GinkgoHelper()
		if shares {
			return vmGroups(ctx, fxt, dracpuTesterImage, targetNode.Name, effective, claim)
		}
		exclusive := claimExclusiveCPUs(claim)
		return map[string]expectedGroup{
			"vcpu":   {cpus: exclusive["vcpu-a"].Union(exclusive["vcpu-b"]), cpuMax: "max 100000", weight: "1"},
			"helper": {cpus: exclusive["helper"], cpuMax: "max 100000", weight: "1"},
		}
	}

	ginkgo.It("should build the claim's groups under the container's thread root, and hand their files to the container's user", func(ctx context.Context) {
		fxt := rootFxt.WithPrefix("threaded-tree")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		c := baseClaim("")
		claim := createThreadedClaim(ctx, fxt, "vm", c.spec)
		pod := createThreadedPod(ctx, fxt, threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, claim))
		claim = preparedClaim(ctx, fxt, claim)

		tree := readThreadedTree(ctx, restConfig, fxt, pod)
		ginkgo.By("finding the thread root where the container's init runs, through /var/run/dracpu/cgroup")
		expectLinkTo(tree, tree.InitCgroup)
		expectGroups(tree, baseGroups(ctx, fxt, claim))
		expectClaimGroups(ctx, restConfig, fxt, claim, pod, c.groups)
		expectOwnedShareIDs(claim)
	})

	ginkgo.It("should move the container's process into its place group", func(ctx context.Context) {
		if string(cfg.CgroupThreadedMode) != cgroupfs.ThreadedModePlace {
			ginkgo.Skip("the driver runs cgroupThreadedMode create, which moves no process")
		}
		place := "helper"
		if shares {
			place = "vmm-cpu"
		}
		// The kernel refuses the move into a group of other CPUs, unless the
		// driver holds CAP_SYS_NICE, for a process whose permitted capabilities
		// exceed the driver's: a root, a privileged and a non-root container
		// given a capability the driver lacks hold more than it does.
		for _, tc := range []struct {
			name   string
			prefix string
			user   int64
			adjust func(*v1.Pod)
		}{
			{name: "a non-root container without capabilities", prefix: "user", user: threadedUser, adjust: func(*v1.Pod) {}},
			{name: "a root container with the runtime's default capabilities", prefix: "root", user: 0, adjust: func(pod *v1.Pod) {
				pod.Spec.SecurityContext = &v1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0))}
				pod.Spec.Containers[0].SecurityContext = nil
			}},
			{name: "a privileged container", prefix: "privileged", user: 0, adjust: func(pod *v1.Pod) {
				pod.Spec.SecurityContext = &v1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsGroup: new(int64(0))}
				pod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{Privileged: new(true)}
			}},
			{name: "a non-root container given a capability", prefix: "added", user: threadedUser, adjust: func(pod *v1.Pod) {
				pod.Spec.Containers[0].SecurityContext.Capabilities.Add = []v1.Capability{"NET_ADMIN"}
			}},
		} {
			ginkgo.By("running " + tc.name)
			fxt := rootFxt.WithPrefix("threaded-place-" + tc.prefix)
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			c := baseClaim(place)
			claim := createThreadedClaim(ctx, fxt, "vm", c.spec)
			pod := threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, claim)
			tc.adjust(pod)
			pod = createThreadedPod(ctx, fxt, pod)
			claim = preparedClaim(ctx, fxt, claim)

			tree := readThreadedTree(ctx, restConfig, fxt, pod)
			gomega.Expect(path.Base(tree.InitCgroup)).To(gomega.Equal(place), "%s: the container's init runs in %s, not in its place group", tc.name, tree.InitCgroup)
			expectLinkTo(tree, path.Dir(tree.InitCgroup))
			expectGroupsOwnedBy(tree, baseGroups(ctx, fxt, claim), cgroupOwner{UID: int(tc.user), GID: int(tc.user)})
			gomega.Expect(fxt.Teardown(ctx)).To(gomega.Succeed())
		}
	})

	ginkgo.It("should let the workload move its pinned threads into their group, and leave its other threads in its control group", func(ctx context.Context) {
		if !shares {
			ginkgo.Skip("node " + targetNode.Name + " gives no shares, which a control group holds")
		}
		fxt := rootFxt.WithPrefix("threaded-poll")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		// SPDK's and DOCA's shape: control threads on a share, pollers on
		// exclusive CPUs, each poller moving itself into the exclusive group
		// before it pins itself. Under create the workload moves its process
		// into the control group itself.
		command := []string{"/dracpucgroup", "workload", "-pin", "poll", "-threads", "2"}
		place := ""
		if string(cfg.CgroupThreadedMode) == cgroupfs.ThreadedModePlace {
			place = "control"
		} else {
			command = append(command, "-move", "control")
		}
		spec := resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
			exactCPURequest("control", driverName, "250m"),
			exactCPURequest("poll", driverName, "2"),
		}}}
		spec.Devices.Config = []resourcev1.DeviceClaimConfiguration{
			opaqueEntry([]string{"poll"}, map[string]any{"cpuConfig": map[string]any{"exclusive": true}}),
			threadedEntry(place,
				v1alpha1.ThreadedGroup{Name: "control", Requests: []string{"control"}},
				v1alpha1.ThreadedGroup{Name: "poll", Requests: []string{"poll"}},
			),
		}
		claim := createThreadedClaim(ctx, fxt, "spdk", spec)
		pod := createThreadedPod(ctx, fxt, threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, command, claim))
		claim = preparedClaim(ctx, fxt, claim)
		pollCPUs := claimExclusiveCPUs(claim)["poll"]
		gomega.Expect(pollCPUs.Size()).To(gomega.Equal(2), "request poll holds CPUs %s", pollCPUs)

		tree := readThreadedTree(ctx, restConfig, fxt, pod)
		root := strings.TrimPrefix(tree.Resolved, cgroupMount)
		if root == "" {
			root = "/"
		}
		var report workloadReport
		gomega.Eventually(func(g gomega.Gomega) {
			logs, err := e2epod.GetLogs(ctx, fxt.K8SClientset, pod)
			g.Expect(err).ToNot(gomega.HaveOccurred())
			g.Expect(unmarshalLatestReport(logs, &report)).To(gomega.Succeed())
			g.Expect(report.Errors).To(gomega.BeEmpty(), "the workload failed to place its threads")
			g.Expect(report.Pinned).To(gomega.HaveLen(2))
		}).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())

		ginkgo.By("finding each pinned thread in the exclusive group, on its CPUs, and every other thread in the control group")
		var pinned, others []int
		for _, t := range report.Pinned {
			gomega.Expect(t.Cgroup).To(gomega.Equal(path.Join(root, "poll")), "pinned thread %d runs in %s", t.TID, t.Cgroup)
			gomega.Expect(cpuset.Parse(t.CPUs)).To(gomega.Equal(pollCPUs), "pinned thread %d runs on CPUs %s", t.TID, t.CPUs)
			pinned = append(pinned, t.TID)
		}
		for _, t := range report.Others {
			gomega.Expect(t.Cgroup).To(gomega.Equal(path.Join(root, "control")), "thread %d runs in %s", t.TID, t.Cgroup)
			others = append(others, t.TID)
		}
		tree = readThreadedTree(ctx, restConfig, fxt, pod)
		groups := groupsByName(tree)
		gomega.Expect(groups).To(gomega.HaveKey("poll"))
		gomega.Expect(groups).To(gomega.HaveKey("control"))
		gomega.Expect(groups["poll"].Threads).To(gomega.ConsistOf(pinned), "the exclusive group holds other threads than the pinned ones")
		gomega.Expect(groups["control"].Threads).To(gomega.ContainElements(others), "the control group lacks threads of the workload")
		for _, tid := range slices.Concat(pinned, others) {
			gomega.Expect(tree.Root.Threads).ToNot(gomega.ContainElement(tid), "thread %d of the workload stayed in the thread root", tid)
		}
	})

	ginkgo.It("should name the group each alternative's CPUs landed in, and build no group that received none", func(ctx context.Context) {
		if !shares {
			ginkgo.Skip("node " + targetNode.Name + " gives no shares, which one alternative asks for")
		}
		partition, found := singleThreadPartition(effective)
		if !found {
			ginkgo.Skip("node " + targetNode.Name + " has no partition of one thread per core, whose class gives the owned alternative")
		}
		owned := driverName + "." + partition.Name

		// A VM's claim whose helper gets a share or exclusive CPUs: its shared
		// alternative in a group of its own, its exclusive one in vcpu's. The
		// scheduler tries the alternatives in order, so each claim lists first
		// the one it gets.
		for _, tc := range []struct {
			name   string
			first  resourcev1.DeviceSubRequest
			second resourcev1.DeviceSubRequest
			want   map[string]string
			built  []string
		}{
			{
				name:   "shared-first",
				first:  cpuSubRequest("shared", driverName, "500m"),
				second: cpuSubRequest("owned", owned, "1"),
				want:   map[string]string{"vcpu": "vcpu", "helper": "helper"},
				built:  []string{"helper", "vcpu"},
			},
			{
				name:   "owned-first",
				first:  cpuSubRequest("owned", owned, "1"),
				second: cpuSubRequest("shared", driverName, "500m"),
				want:   map[string]string{"vcpu": "vcpu", "helper": "vcpu"},
				built:  []string{"vcpu"},
			},
		} {
			ginkgo.By("running a claim that lists helper's alternatives " + tc.name)
			fxt := rootFxt.WithPrefix("threaded-" + tc.name)
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			spec := resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
				exactCPURequest("vcpu", driverName, "1"),
				{Name: "helper", FirstAvailable: []resourcev1.DeviceSubRequest{tc.first, tc.second}},
			}}}
			spec.Devices.Config = []resourcev1.DeviceClaimConfiguration{
				opaqueEntry([]string{"vcpu"}, map[string]any{"cpuConfig": map[string]any{"exclusive": true}}),
				threadedEntry("",
					v1alpha1.ThreadedGroup{Name: "vcpu", Requests: []string{"vcpu", "helper/owned"}},
					v1alpha1.ThreadedGroup{Name: "helper", Requests: []string{"helper/shared"}},
				),
			}
			claim := createThreadedClaim(ctx, fxt, "split", spec)
			pod := createThreadedPod(ctx, fxt, threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, claim))
			claim = preparedClaim(ctx, fxt, claim)

			tree := readThreadedTree(ctx, restConfig, fxt, pod)
			gomega.Expect(slices.Sorted(maps.Keys(groupsByName(tree)))).To(gomega.Equal(tc.built), "claim %s", tc.name)
			expectClaimGroups(ctx, restConfig, fxt, claim, pod, tc.want)
		}
	})

	ginkgo.It("should keep the trees and links across a driver restart, and repair a deleted group and a wrong link without moving or killing a container", func(ctx context.Context) {
		if !kindNodeReachable(ctx, targetNode.Name) {
			ginkgo.Skip("docker reaches no container of node " + targetNode.Name + ", in which the spec breaks a tree; run it on a kind cluster")
		}
		fxt := rootFxt.WithPrefix("threaded-restart")
		gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
		ginkgo.DeferCleanup(fxt.Teardown)

		c := baseClaim("")
		claim := createThreadedClaim(ctx, fxt, "vm", c.spec)
		threaded := createThreadedPod(ctx, fxt, threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, claim))
		claim = preparedClaim(ctx, fxt, claim)
		unthreaded := createTesterPodWithClaimSpec(ctx, fxt, dracpuTesterImage, targetNode.Name, makeResourceClaimSpec(1, cfg.CPUDeviceMode == "grouped"), "unthreaded")
		want := baseGroups(ctx, fxt, claim)
		before := readThreadedTree(ctx, restConfig, fxt, threaded)
		unthreadedCPUs := getTesterPodCPUAllocation(fxt.K8SClientset, ctx, unthreaded).CPUAssigned

		after := breakAndRepairTree(ctx, fxt, restConfig, targetNode.Name, threaded, "vcpu")
		expectLinkTo(after, after.InitCgroup)
		expectGroups(after, want)
		gomega.Expect(after.InitCgroup).To(gomega.Equal(before.InitCgroup), "the driver moved the container's process")

		ginkgo.By("finding both containers running as before, on the same CPUs")
		for _, pod := range []*v1.Pod{threaded, unthreaded} {
			current, err := fxt.K8SClientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			gomega.Expect(err).ToNot(gomega.HaveOccurred())
			gomega.Expect(current.Status.ContainerStatuses[0].ContainerID).To(gomega.Equal(pod.Status.ContainerStatuses[0].ContainerID), "pod %s runs another container", e2epod.Identify(pod))
			gomega.Expect(current.Status.ContainerStatuses[0].RestartCount).To(gomega.Equal(pod.Status.ContainerStatuses[0].RestartCount), "pod %s restarted", e2epod.Identify(pod))
		}
		gomega.Expect(getTesterPodCPUAllocation(fxt.K8SClientset, ctx, unthreaded).CPUAssigned).To(gomega.Equal(unthreadedCPUs), "the unthreaded claim's container moved")
		expectClaimGroups(ctx, restConfig, fxt, preparedClaim(ctx, fxt, claim), threaded, c.groups)
	})
})

// On a crun lane the specs above run under crun, and these run the cases
// crun's two annotations on the container's cgroup make. The thread root is
// the cgroup the container's init runs in, below the container's cgroup:
// crun's sub-cgroup under the systemd driver, container unless
// run.oci.systemd.subgroup names another, and run.oci.delegate-cgroup's path
// below that (crun 1.30.1 src/libcrun/cgroup-systemd.c:2017-2031,
// src/libcrun/cgroup.c:475). The lanes install cgroupThreadedMode place.
var _ = ginkgo.Describe("Threaded cgroups under crun", ginkgo.Label("threaded", "crun"), ginkgo.Serial, ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var (
		rootFxt             *fixture.Fixture
		restConfig          *rest.Config
		dracpuTesterImage   string
		targetNode          *v1.Node
		effective           driverConfigValues
		cgroupDriver        string
		createdRuntimeClass bool
		runtimeHandler      = threadedRuntimeHandler()
	)

	ginkgo.BeforeAll(func(ctx context.Context) {
		if !strings.HasPrefix(runtimeHandler, "crun") {
			ginkgo.Skip("the threaded specs run under the runtime handler " + runtimeHandler + "; create the cluster with DRACPU_E2E_LANE=crun-systemd or crun-cgroupfs")
		}
		dracpuTesterImage = os.Getenv("DRACPU_E2E_TEST_IMAGE")
		gomega.Expect(dracpuTesterImage).ToNot(gomega.BeEmpty(), "missing environment variable DRACPU_E2E_TEST_IMAGE")
		var err error
		rootFxt, err = fixture.ForGinkgo()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create fixture")
		restConfig, err = e2eclient.NewK8SConfig()
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create Kubernetes config")

		cfg := getDriverConfig(ctx, rootFxt.K8SClientset)
		if string(cfg.CgroupThreadedMode) != cgroupfs.ThreadedModePlace {
			ginkgo.Skip("the driver runs cgroupThreadedMode " + cfg.CgroupThreadedMode.String() + "; the crun lanes install place")
		}
		nodes, err := rootFxt.K8SClientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: driverconfig.ProfileLabel + "=" + allowingProfile})
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		if len(nodes.Items) == 0 {
			ginkgo.Skip("no node runs the profile " + allowingProfile + ", whose default partition gives exclusive CPUs and shares")
		}
		targetNode = &nodes.Items[0]
		effective = cfg.effectiveFor(targetNode)
		if !kindNodeReachable(ctx, targetNode.Name) {
			ginkgo.Skip("docker reaches no container of node " + targetNode.Name + ", whose cgroups the specs read; run them on a kind cluster")
		}
		cgroupDriver = kubeletCgroupDriver(ctx, rootFxt, targetNode.Name)
		rootFxt.Log.Info("using worker node", "nodeName", targetNode.Name, "cgroupDriver", cgroupDriver)
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		createdRuntimeClass = createRuntimeClass(ctx, rootFxt, runtimeHandler)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if createdRuntimeClass {
			deleteRuntimeClass(ctx, rootFxt, runtimeHandler)
		}
	})

	ginkgo.It("should find the driver's own process in crun's sub-cgroup at start", func(ctx context.Context) {
		if cgroupDriver != "systemd" {
			ginkgo.Skip("crun makes a sub-cgroup under the systemd driver only")
		}
		driverPod, err := e2epod.GetDRACPUPod(ctx, rootFxt.K8SClientset, targetNode.Name)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(driverPod.Spec.RuntimeClassName).To(gomega.HaveValue(gomega.Equal(runtimeHandler)), "the driver's pod does not run under crun")
		logs, err := e2epod.GetLogs(ctx, rootFxt.K8SClientset, driverPod)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		var found []string
		for line := range strings.Lines(logs) {
			if strings.Contains(line, "found the driver's process in the host's cgroup2 tree") {
				if m := regexp.MustCompile(`cgroup="([^"]+)"`).FindStringSubmatch(line); m != nil {
					found = append(found, m[1])
				}
			}
		}
		gomega.Expect(found).To(gomega.HaveLen(1), "the driver logged where it found its process %d times", len(found))
		gomega.Expect(path.Base(found[0])).To(gomega.Equal("container"), "the driver found its process in %s", found[0])
	})

	for _, tc := range []struct {
		name        string
		annotations map[string]string
		systemdOnly bool
		// below returns the thread root's path below the container's cgroup
		// on the host, under the kubelet's cgroup driver.
		below func(driver string) string
		// root is the thread root as the container's cgroup namespace shows it.
		root string
	}{
		{
			name: "no annotation",
			below: func(driver string) string {
				if driver == "systemd" {
					return "container"
				}
				return ""
			},
			root: "/",
		},
		{
			name:        "run.oci.systemd.subgroup naming another sub-cgroup",
			annotations: map[string]string{"run.oci.systemd.subgroup": "payload"},
			systemdOnly: true,
			below:       func(string) string { return "payload" },
			root:        "/",
		},
		{
			name:        "run.oci.delegate-cgroup naming a nested path",
			annotations: map[string]string{"run.oci.delegate-cgroup": "outer/inner"},
			below: func(driver string) string {
				if driver == "systemd" {
					return "container/outer/inner"
				}
				return "outer/inner"
			},
			root: "/outer/inner",
		},
	} {
		ginkgo.It("should build, place and repair the groups under the init's cgroup with "+tc.name, func(ctx context.Context) {
			if tc.systemdOnly && cgroupDriver != "systemd" {
				ginkgo.Skip("crun reads run.oci.systemd.subgroup under the systemd driver only")
			}
			fxt := rootFxt.WithPrefix("threaded-crun")
			gomega.Expect(fxt.Setup(ctx)).To(gomega.Succeed())
			ginkgo.DeferCleanup(fxt.Teardown)

			c := vmClaim("vmm-cpu")
			claim := createThreadedClaim(ctx, fxt, "vm", c.spec)
			pod := threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, claim)
			pod.Annotations = tc.annotations
			pod = createThreadedPod(ctx, fxt, pod)
			claim = preparedClaim(ctx, fxt, claim)

			// Under a delegate path an exec'd process joins the init's
			// current cgroup, so a second container, whose claim names no
			// place, shows it before any move. Its exclusive CPU leaves the
			// first container's shares, so it runs before they are read.
			_, delegated := tc.annotations["run.oci.delegate-cgroup"]
			var other *v1.Pod
			var otherExclusive cpuset.CPUSet
			if delegated {
				unplaced := createThreadedClaim(ctx, fxt, "unplaced", vmClaim("").spec)
				other = threadedPod(fxt.Namespace.Name, dracpuTesterImage, targetNode.Name, runtimeHandler, []string{"/dracputester"}, unplaced)
				other.Annotations = tc.annotations
				other = createThreadedPod(ctx, fxt, other)
				otherExclusive = claimExclusiveCPUs(preparedClaim(ctx, fxt, unplaced))["vcpu"]
			}
			want := vmGroups(ctx, fxt, dracpuTesterImage, targetNode.Name, effective, claim, otherExclusive)

			tree := readThreadedTree(ctx, restConfig, fxt, pod)
			placed := path.Join(tc.root, "vmm-cpu")
			gomega.Expect(tree.InitCgroup).To(gomega.Equal(placed), "the container's init does not run in its place group")
			expectLinkTo(tree, tc.root)
			expectGroups(tree, want)
			expectClaimGroups(ctx, restConfig, fxt, claim, pod, c.groups)

			ginkgo.By("finding the thread root below the container's cgroup on the node")
			id := containerIDOf(pod)
			container := strings.TrimSpace(nodeShell(ctx, targetNode.Name, fmt.Sprintf("find %s -type d -name '*%s*'", cgroupMount, id)))
			gomega.Expect(strings.Fields(container)).To(gomega.HaveLen(1), "node %s has not one cgroup named after container %s", targetNode.Name, id)
			group := strings.TrimSpace(nodeShell(ctx, targetNode.Name, fmt.Sprintf("find %s -type d -name vcpu", container)))
			gomega.Expect(path.Dir(group)).To(gomega.Equal(path.Join(container, tc.below(cgroupDriver))), "the thread root on node %s", targetNode.Name)

			// An exec'd process joins the container's cgroup, crun's
			// sub-cgroup under the systemd driver, and under a delegate
			// path, which the container's cgroup refuses, the init's current
			// cgroup (crun 1.30.1 src/libcrun/cgroup-setup.c:448-475).
			execWant := "/"
			if delegated {
				execWant = placed
				otherTree := readThreadedTree(ctx, restConfig, fxt, other)
				gomega.Expect(otherTree.InitCgroup).To(gomega.Equal(tc.root), "the init of a claim without place left its thread root")
				gomega.Expect(otherTree.ExecCgroup).To(gomega.Equal(tc.root), "an exec'd process before any move")
			}
			gomega.Expect(tree.ExecCgroup).To(gomega.Equal(execWant), "an exec'd process")

			if cgroupDriver == "systemd" {
				ginkgo.By("reloading systemd in node " + targetNode.Name)
				nodeShell(ctx, targetNode.Name, "systemctl daemon-reload")
				reloaded := readThreadedTree(ctx, restConfig, fxt, pod)
				gomega.Expect(groupSettings(reloaded)).To(gomega.Equal(groupSettings(tree)), "systemd changed the groups' CPUs or limits")
			}

			after := breakAndRepairTree(ctx, fxt, restConfig, targetNode.Name, pod, "vcpu")
			gomega.Expect(after.InitCgroup).To(gomega.Equal(placed), "the driver moved the container's process")
			expectLinkTo(after, tc.root)
			expectGroups(after, want)
		})
	}
})

// groupSettings returns each group's CPUs and cpu.max.
func groupSettings(tree threadedTree) map[string][2]string {
	settings := map[string][2]string{}
	for _, g := range tree.Groups {
		settings[g.Name] = [2]string{g.CPUs, g.CPUMax}
	}
	return settings
}

// kubeletCgroupDriver returns the cgroup driver of the node's kubelet, as
// its configz endpoint reports it.
func kubeletCgroupDriver(ctx context.Context, fxt *fixture.Fixture, nodeName string) string {
	ginkgo.GinkgoHelper()
	raw, err := fxt.K8SClientset.CoreV1().RESTClient().Get().AbsPath("/api/v1/nodes", nodeName, "proxy", "configz").DoRaw(ctx)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot read the kubelet configuration of node %s", nodeName)
	var configz struct {
		KubeletConfig struct {
			CgroupDriver string `json:"cgroupDriver"`
		} `json:"kubeletconfig"`
	}
	gomega.Expect(json.Unmarshal(raw, &configz)).To(gomega.Succeed())
	gomega.Expect(configz.KubeletConfig.CgroupDriver).ToNot(gomega.BeEmpty(), "node %s reports no cgroup driver", nodeName)
	return configz.KubeletConfig.CgroupDriver
}

// vmClaim is a VM's claim on a node whose default partition gives exclusive
// CPUs and shares: its vCPU, its capped housekeeping and an uncapped share,
// each in a group named after it.
func vmClaim(place string) threadedClaim {
	ginkgo.GinkgoHelper()
	spec := resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{Requests: []resourcev1.DeviceRequest{
		exactCPURequest("vcpu", driverName, "1"),
		exactCPURequest("vmm-cpu", driverName, "250m"),
		exactCPURequest("io", driverName, "500m"),
	}}}
	spec.Devices.Config = []resourcev1.DeviceClaimConfiguration{
		opaqueEntry([]string{"vcpu"}, map[string]any{"cpuConfig": map[string]any{"exclusive": true}}),
		opaqueEntry([]string{"vmm-cpu"}, map[string]any{"cpuConfig": map[string]any{"limit": "500m"}}),
		opaqueEntry([]string{"io"}, map[string]any{"cpuConfig": map[string]any{"limit": v1alpha1.LimitMax}}),
		threadedEntry(place),
	}
	return threadedClaim{spec: spec, groups: map[string]string{"vcpu": "vcpu", "vmm-cpu": "vmm-cpu", "io": "io"}}
}

// vmGroups returns what the groups of vmClaim hold on nodeName, beside the
// exclusive CPUs of other claims. A share runs on the default partition's
// CPUs less every exclusive request's, and the groups weigh as
// docs/user/threaded-cgroups.md gives: 1 for exclusive CPUs, and for shares
// of 250m and 500m, 256 and 512 shares as the kubelet counts them, 5000 and
// 10000.
func vmGroups(ctx context.Context, fxt *fixture.Fixture, image, nodeName string, effective driverConfigValues, claim *resourcev1.ResourceClaim, othersExclusive ...cpuset.CPUSet) map[string]expectedGroup {
	ginkgo.GinkgoHelper()
	vcpu := claimExclusiveCPUs(claim)["vcpu"]
	shared := defaultPartitionCPUs(ctx, fxt, image, nodeName, effective).Difference(vcpu)
	for _, cpus := range othersExclusive {
		shared = shared.Difference(cpus)
	}
	return map[string]expectedGroup{
		"vcpu":    {cpus: vcpu, cpuMax: "max 100000", weight: "1"},
		"vmm-cpu": {cpus: shared, cpuMax: "50000 100000", weight: "5000"},
		"io":      {cpus: shared, cpuMax: "max 100000", weight: "10000"},
	}
}

// breakAndRepairTree keeps the driver off the node while it deletes the
// pod's group broken, which must hold no thread, and points the pod's link
// elsewhere, makes sure both happened, brings the driver back, and returns
// the tree once the driver has repaired both.
func breakAndRepairTree(ctx context.Context, fxt *fixture.Fixture, restConfig *rest.Config, nodeName string, pod *v1.Pod, broken string) threadedTree {
	ginkgo.GinkgoHelper()
	before := readThreadedTree(ctx, restConfig, fxt, pod)
	leaveDaemonSetOff(ctx, fxt.K8SClientset, nodeName)
	id := containerIDOf(pod)
	ginkgo.By("deleting group " + broken + " and pointing the link elsewhere while the driver is away")
	dir := strings.TrimSpace(nodeShell(ctx, nodeName, fmt.Sprintf("find %s -type d -path '*%s*' -name %s", cgroupMount, id, broken)))
	gomega.Expect(dir).ToNot(gomega.BeEmpty(), "node %s has no cgroup %s of container %s", nodeName, broken, id)
	nodeShell(ctx, nodeName, "rmdir "+dir)
	linkFile := path.Join(kubeletRootDir(ctx, fxt), cgroupLinks, id, "cgroup")
	nodeShell(ctx, nodeName, "ln -sfn "+cgroupMount+"/elsewhere "+linkFile)
	gomega.Expect(nodeShell(ctx, nodeName, "test -e "+dir+" || echo gone")).To(gomega.ContainSubstring("gone"), "group %s was not deleted", broken)
	gomega.Expect(strings.TrimSpace(nodeShell(ctx, nodeName, "readlink "+linkFile))).To(gomega.Equal(cgroupMount+"/elsewhere"), "the link was not changed")

	restoreDaemonSetNodes(ctx, fxt.K8SClientset, nodeName)
	ginkgo.By("finding the group and the link repaired once the driver has synchronized with the runtime")
	gomega.Eventually(func(g gomega.Gomega) {
		tree := readThreadedTree(ctx, restConfig, fxt, pod)
		g.Expect(tree.Link).To(gomega.Equal(before.Link))
		g.Expect(groupsByName(tree)).To(gomega.HaveKey(broken))
		g.Expect(nodeShell(ctx, nodeName, "test -d "+dir+" && echo back || true")).To(gomega.ContainSubstring("back"), "group %s is not back where it was", broken)
	}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(gomega.Succeed())
	return readThreadedTree(ctx, restConfig, fxt, pod)
}

func exactCPURequest(name, class, amount string) resourcev1.DeviceRequest {
	request := resourcev1.DeviceRequest{Name: name, Exactly: &resourcev1.ExactDeviceRequest{DeviceClassName: class}}
	if amount == "" {
		request.Exactly.Count = 1
		return request
	}
	request.Exactly.Capacity = &resourcev1.CapacityRequirements{Requests: map[resourcev1.QualifiedName]resource.Quantity{
		device.CPUResourceQualifiedName: resource.MustParse(amount),
	}}
	return request
}

func cpuSubRequest(name, class, amount string) resourcev1.DeviceSubRequest {
	return resourcev1.DeviceSubRequest{Name: name, DeviceClassName: class, Capacity: &resourcev1.CapacityRequirements{
		Requests: map[resourcev1.QualifiedName]resource.Quantity{
			device.CPUResourceQualifiedName: resource.MustParse(amount),
		},
	}}
}

// opaqueEntry is a config entry of this driver for requests, nil for the
// whole claim, holding only the given parameters.
func opaqueEntry(requests []string, parameters map[string]any) resourcev1.DeviceClaimConfiguration {
	ginkgo.GinkgoHelper()
	parameters["apiVersion"] = v1alpha1.APIVersion
	raw, err := json.Marshal(parameters)
	gomega.Expect(err).ToNot(gomega.HaveOccurred())
	return resourcev1.DeviceClaimConfiguration{
		Requests: requests,
		DeviceConfiguration: resourcev1.DeviceConfiguration{Opaque: &resourcev1.OpaqueDeviceConfiguration{
			Driver: driverName, Parameters: runtime.RawExtension{Raw: raw},
		}},
	}
}

// threadedEntry switches the threaded split on for the whole claim.
func threadedEntry(place string, groups ...v1alpha1.ThreadedGroup) resourcev1.DeviceClaimConfiguration {
	ginkgo.GinkgoHelper()
	return opaqueEntry(nil, map[string]any{"cgroupConfig": map[string]any{
		"threaded": v1alpha1.ThreadedConfig{Enabled: true, Place: place, Groups: groups},
	}})
}

func createThreadedClaim(ctx context.Context, fxt *fixture.Fixture, name string, spec resourcev1.ResourceClaimSpec) *resourcev1.ResourceClaim {
	ginkgo.GinkgoHelper()
	claim, err := fxt.K8SClientset.ResourceV1().ResourceClaims(fxt.Namespace.Name).Create(ctx,
		&resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: fxt.Namespace.Name}, Spec: spec}, metav1.CreateOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create claim %s", name)
	return claim
}

// threadedRuntimeHandler is the runtime handler the threaded specs run their
// pods under, and the name of its RuntimeClass.
func threadedRuntimeHandler() string {
	if handler := os.Getenv("DRACPU_E2E_RUNTIME_HANDLER"); handler != "" {
		return handler
	}
	return cgroupWritable
}

// createRuntimeClass creates a RuntimeClass of the handler's name, and reports
// whether it did: a lane may have created it for pods of its own.
func createRuntimeClass(ctx context.Context, fxt *fixture.Fixture, handler string) bool {
	ginkgo.GinkgoHelper()
	_, err := fxt.K8SClientset.NodeV1().RuntimeClasses().Create(ctx, &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: handler},
		Handler:    handler,
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return false
	}
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot create RuntimeClass %s", handler)
	return true
}

func deleteRuntimeClass(ctx context.Context, fxt *fixture.Fixture, name string) {
	ginkgo.GinkgoHelper()
	err := fxt.K8SClientset.NodeV1().RuntimeClasses().Delete(ctx, name, metav1.DeleteOptions{})
	if !apierrors.IsNotFound(err) {
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot delete RuntimeClass %s", name)
	}
}

// threadedPod runs command, as threadedUser and without privileges, under the
// RuntimeClass runtimeClass, in one container holding every claim.
func threadedPod(ns, image, nodeName, runtimeClass string, command []string, claims ...*resourcev1.ResourceClaim) *v1.Pod {
	ginkgo.GinkgoHelper()
	pod := makeTesterPodWithClaimsWithoutCPURequest(ns, image, nodeName, claims...)
	pod.GenerateName = "tester-pod-threaded-"
	pod.Spec.RuntimeClassName = new(runtimeClass)
	pod.Spec.SecurityContext = &v1.PodSecurityContext{RunAsUser: new(int64(threadedUser)), RunAsGroup: new(int64(threadedUser)), RunAsNonRoot: new(true)}
	pod.Spec.Containers[0].Command = command
	pod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{
		AllowPrivilegeEscalation: new(false),
		Capabilities:             &v1.Capabilities{Drop: []v1.Capability{"ALL"}},
	}
	return e2epod.PinToNode(pod, nodeName)
}

func createThreadedPod(ctx context.Context, fxt *fixture.Fixture, pod *v1.Pod) *v1.Pod {
	ginkgo.GinkgoHelper()
	created, err := e2epod.CreateSync(ctx, fxt.K8SClientset, pod)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot run the threaded tester pod")
	return created
}

// preparedClaim returns the claim once the driver has written an entry in its
// status for each of its results on this driver's devices.
func preparedClaim(ctx context.Context, fxt *fixture.Fixture, claim *resourcev1.ResourceClaim) *resourcev1.ResourceClaim {
	ginkgo.GinkgoHelper()
	var current *resourcev1.ResourceClaim
	gomega.Eventually(func(g gomega.Gomega) {
		var err error
		current, err = fxt.K8SClientset.ResourceV1().ResourceClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
		g.Expect(err).ToNot(gomega.HaveOccurred())
		g.Expect(current.Status.Allocation).ToNot(gomega.BeNil(), "claim %s is not allocated", claim.Name)
		results := 0
		for _, result := range current.Status.Allocation.Devices.Results {
			if result.Driver == driverName {
				results++
			}
		}
		g.Expect(current.Status.Devices).To(gomega.HaveLen(results), "claim %s has not an entry for each result", claim.Name)
	}).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(gomega.Succeed())
	return current
}

// claimStatusByRequest returns each status entry of the claim with its data,
// by the request, or the request an alternative belongs to, of its result.
func claimStatusByRequest(claim *resourcev1.ResourceClaim) map[string][]resourcev1.AllocatedDeviceStatus {
	ginkgo.GinkgoHelper()
	byRequest := map[string][]resourcev1.AllocatedDeviceStatus{}
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != driverName {
			continue
		}
		idx := slices.IndexFunc(claim.Status.Devices, func(entry resourcev1.AllocatedDeviceStatus) bool {
			return entry.Driver == result.Driver && entry.Pool == result.Pool && entry.Device == result.Device &&
				ptrEqual(entry.ShareID, (*string)(result.ShareID))
		})
		gomega.Expect(idx).ToNot(gomega.Equal(-1), "claim %s has no status entry for its result on device %s", claim.Name, result.Device)
		request := resourceclaim.BaseRequestRef(result.Request)
		byRequest[request] = append(byRequest[request], claim.Status.Devices[idx])
	}
	return byRequest
}

func placementOf(entry resourcev1.AllocatedDeviceStatus) v1alpha1.ClaimPlacementStatus {
	ginkgo.GinkgoHelper()
	gomega.Expect(entry.Data).ToNot(gomega.BeNil(), "status entry of device %s has no data", entry.Device)
	var placement v1alpha1.ClaimPlacementStatus
	gomega.Expect(json.Unmarshal(entry.Data.Raw, &placement)).To(gomega.Succeed())
	return placement
}

// claimExclusiveCPUs returns the CPUs each request holds of its own, as its
// status entries name them.
func claimExclusiveCPUs(claim *resourcev1.ResourceClaim) map[string]cpuset.CPUSet {
	ginkgo.GinkgoHelper()
	cpus := map[string]cpuset.CPUSet{}
	for request, entries := range claimStatusByRequest(claim) {
		held := cpuset.New()
		for _, entry := range entries {
			if placement := placementOf(entry); placement.CPUSet != "" {
				parsed, err := cpuset.Parse(placement.CPUSet)
				gomega.Expect(err).ToNot(gomega.HaveOccurred())
				held = held.Union(parsed)
			}
		}
		cpus[request] = held
	}
	return cpus
}

// expectClaimGroups checks that each request's status entries and metadata
// file name the group its result landed in.
func expectClaimGroups(ctx context.Context, restConfig *rest.Config, fxt *fixture.Fixture, claim *resourcev1.ResourceClaim, pod *v1.Pod, want map[string]string) {
	ginkgo.GinkgoHelper()
	byRequest := claimStatusByRequest(claim)
	gomega.Expect(slices.Sorted(maps.Keys(byRequest))).To(gomega.Equal(slices.Sorted(maps.Keys(want))), "claim %s", claim.Name)
	for request, entries := range byRequest {
		for _, entry := range entries {
			gomega.Expect(placementOf(entry).Group).To(gomega.Equal(want[request]), "the status entry of request %s of claim %s", request, claim.Name)
		}
	}
	metadata := readMetadataDevices(ctx, restConfig, fxt, pod)
	for request, group := range want {
		var devices []metadataDevice
		for name, devs := range metadata {
			if resourceclaim.BaseRequestRef(name) == request {
				devices = append(devices, devs...)
			}
		}
		gomega.Expect(devices).ToNot(gomega.BeEmpty(), "no metadata file names request %s", request)
		for _, dev := range devices {
			expectStringAttrValue(dev.Attributes, "dra.cpu/group", group)
		}
	}
}

// expectOwnedShareIDs checks that each status entry of a request with CPUs of
// its own carries its result's share, as the API server keys the entries.
func expectOwnedShareIDs(claim *resourcev1.ResourceClaim) {
	ginkgo.GinkgoHelper()
	exclusive := claimExclusiveCPUs(claim)
	for _, result := range claim.Status.Allocation.Devices.Results {
		if result.Driver != driverName || exclusive[resourceclaim.BaseRequestRef(result.Request)].IsEmpty() {
			continue
		}
		idx := slices.IndexFunc(claim.Status.Devices, func(entry resourcev1.AllocatedDeviceStatus) bool {
			return entry.Pool == result.Pool && entry.Device == result.Device && ptrEqual(entry.ShareID, (*string)(result.ShareID))
		})
		gomega.Expect(idx).ToNot(gomega.Equal(-1), "request %s of claim %s has no status entry with its result's share", result.Request, claim.Name)
	}
}

func ptrEqual(a, b *string) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

func readThreadedTree(ctx context.Context, restConfig *rest.Config, fxt *fixture.Fixture, pod *v1.Pod) threadedTree {
	ginkgo.GinkgoHelper()
	stdout, stderr, err := e2epod.Exec(ctx, restConfig, fxt.K8SClientset, pod, "/dracpucgroup", "tree")
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "dracpucgroup tree failed; stdout: %s; stderr: %s", stdout, stderr)
	var tree threadedTree
	gomega.Expect(json.Unmarshal([]byte(stdout), &tree)).To(gomega.Succeed(), "cannot parse dracpucgroup's output: %s", stdout)
	return tree
}

func groupsByName(tree threadedTree) map[string]cgroupView {
	groups := map[string]cgroupView{}
	for _, g := range tree.Groups {
		groups[g.Name] = g
	}
	return groups
}

// expectLinkTo checks that /var/run/dracpu/cgroup names root, a cgroup as the
// container's cgroup namespace shows it, and that root is the thread root.
func expectLinkTo(tree threadedTree, root string) {
	ginkgo.GinkgoHelper()
	want := path.Join(cgroupMount, root)
	gomega.Expect(tree.Link).To(gomega.Equal(want), "/var/run/dracpu/cgroup")
	gomega.Expect(tree.Resolved).To(gomega.Equal(want), "/var/run/dracpu/cgroup resolves elsewhere")
	gomega.Expect(tree.Root.Type).To(gomega.Equal("domain threaded"), "%s is no thread root", want)
	gomega.Expect(strings.Fields(tree.Root.SubtreeControl)).To(gomega.ContainElements("cpu", "cpuset"), "%s does not give its groups cpu and cpuset", want)
}

// expectGroups checks that the thread root holds exactly the groups of want,
// each threaded and set as want gives, that the files moving processes and
// threads into a group belong to the container's user and every other file
// to root, and that the thread root's cgroup.procs is handed over.
func expectGroups(tree threadedTree, want map[string]expectedGroup) {
	ginkgo.GinkgoHelper()
	expectGroupsOwnedBy(tree, want, cgroupOwner{UID: threadedUser, GID: threadedUser})
}

// expectGroupsOwnedBy is expectGroups for a container whose process runs as
// user.
func expectGroupsOwnedBy(tree threadedTree, want map[string]expectedGroup, user cgroupOwner) {
	ginkgo.GinkgoHelper()
	groups := groupsByName(tree)
	gomega.Expect(slices.Sorted(maps.Keys(groups))).To(gomega.Equal(slices.Sorted(maps.Keys(want))), "the thread root's groups")
	root := cgroupOwner{}
	all := cpuset.New()
	for name, w := range want {
		g := groups[name]
		gomega.Expect(g.Type).To(gomega.Equal("threaded"), "group %s", name)
		gomega.Expect(cpuset.Parse(g.CPUs)).To(gomega.Equal(w.cpus), "the CPUs of group %s", name)
		gomega.Expect(g.CPUMax).To(gomega.Equal(w.cpuMax), "the cpu.max of group %s", name)
		gomega.Expect(g.Weight).To(gomega.Equal(w.weight), "the cpu.weight of group %s", name)
		gomega.Expect(g.Owners).To(gomega.Equal(map[string]cgroupOwner{
			".": root, "cgroup.procs": user, "cgroup.threads": user,
			"cgroup.subtree_control": root, "cpuset.cpus": root, "cpu.max": root, "cpu.weight": root,
		}), "the owners of group %s's files", name)
		all = all.Union(w.cpus)
	}
	// A thread root below the container's cgroup, as crun makes one, may
	// leave its own cpuset empty and run on the container's.
	gomega.Expect(cpuset.Parse(tree.Root.CPUsEffective)).To(gomega.Equal(all), "the thread root's CPUs")
	// The thread root's directory, cgroup.subtree_control and cgroup.threads
	// are the runtime's: runc under the systemd driver hands them to the
	// process's user where the cgroup mount is writable (v1.5.1
	// libcontainer/specconv/spec_linux.go:575-590).
	for file, owner := range map[string]cgroupOwner{"cgroup.procs": user, "cpuset.cpus": root, "cpu.max": root, "cpu.weight": root} {
		gomega.Expect(tree.Root.Owners).To(gomega.HaveKeyWithValue(file, owner), "the owner of the thread root's %s", file)
	}
}

func containerIDOf(pod *v1.Pod) string {
	ginkgo.GinkgoHelper()
	gomega.Expect(pod.Status.ContainerStatuses).ToNot(gomega.BeEmpty(), "pod %s reports no container", e2epod.Identify(pod))
	_, id, found := strings.Cut(pod.Status.ContainerStatuses[0].ContainerID, "://")
	gomega.Expect(found).To(gomega.BeTrue(), "pod %s reports container %q", e2epod.Identify(pod), pod.Status.ContainerStatuses[0].ContainerID)
	return id
}

// kubeletRootDir returns the kubelet's root directory the driver runs with.
func kubeletRootDir(ctx context.Context, fxt *fixture.Fixture) string {
	ginkgo.GinkgoHelper()
	daemonSet, err := fxt.K8SClientset.AppsV1().DaemonSets(daemonSetNamespace).Get(ctx, "dracpu", metav1.GetOptions{})
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "cannot get dracpu daemonset")
	if dir, ok := findArgInContainer(&daemonSet.Spec.Template.Spec.Containers[0], "--kubelet-root-dir="); ok {
		return dir
	}
	return "/var/lib/kubelet"
}

// nodeShell runs script with sh in the kind node's container.
func nodeShell(ctx context.Context, nodeName, script string) string {
	ginkgo.GinkgoHelper()
	out, err := exec.CommandContext(ctx, "docker", "exec", nodeName, "sh", "-c", script).CombinedOutput()
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "%q failed in node %s: %s", script, nodeName, out)
	return string(out)
}
