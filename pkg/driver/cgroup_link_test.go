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

package driver

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
)

var (
	vmID    = strings.Repeat("a", 64)
	otherID = strings.Repeat("b", 64)
	goneID  = strings.Repeat("c", 64)
)

// linkDriver is sharingDriver under mode with the VM claim prepared, split
// as block says, its init's cgroup in tree.
func linkDriver(t *testing.T, mode, block string, tree *writableTree, reg *prometheus.Registry) (*CPUDriver, *api.PodSandbox) {
	t.Helper()
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	var recorder Recorder
	if reg != nil {
		recorder = cpumetrics.New(reg)
	}
	d := sharingDriver(t, true, nil, recorder)
	d.cgroupThreadedMode = mode
	d.cgroups = tree
	d.ownCgroupFound = true
	d.eventRecorder = &capturingRecorder{}
	d.providers.Processes = fakeProcesses{status: map[int]string{vmInit: vmInitStatus}}
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{vmClaim("vm", block)})
	require.NoError(t, err)
	require.NoError(t, results["vm"].Err)
	return d, &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
}

func ownCgroupNamespace() []*api.LinuxNamespace {
	return []*api.LinuxNamespace{{Type: "cgroup"}, {Type: "mount"}}
}

// CreateContainer gives a container holding a threaded claim, and no other, a
// read-only mount of a directory of its own at /var/run/dracpu, made last, so
// that a refused container leaves none.
func TestCreateContainerMountsTheLinkDirectory(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		claims    []*resourceapi.ResourceClaim
		env       []string
		expiring  bool
		wantMount bool
		wantErr   string
	}{{
		name:      "a container holding a threaded claim",
		mode:      "create",
		claims:    []*resourceapi.ResourceClaim{vmClaim("vm", `{"enabled": true}`)},
		env:       claimEnv("vm", "vcpu", "vmm-cpu"),
		wantMount: true,
	}, {
		name:   "a container holding no threaded claim",
		mode:   "create",
		claims: []*resourceapi.ResourceClaim{claimWithResults("vm", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})},
		env:    claimEnv("vm", "vcpu"),
	}, {
		name: "a refused container",
		mode: "create",
		claims: []*resourceapi.ResourceClaim{vmClaim("vm", `{"enabled": true}`),
			claimWithResults("extra", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"})},
		env:     append(claimEnv("vm", "vcpu", "vmm-cpu"), claimEnv("extra", "io")...),
		wantErr: `container "app": claim "extra" is not threaded but shares the container with threaded claim "vm"; move its requests into the threaded claim or another container`,
	}, {
		name:     "a container the runtime stopped waiting for",
		mode:     "create",
		claims:   []*resourceapi.ResourceClaim{vmClaim("vm", `{"enabled": true}`)},
		env:      claimEnv("vm", "vcpu", "vmm-cpu"),
		expiring: true,
		wantErr:  context.DeadlineExceeded.Error(),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, true, nil, nil)
			d.cgroupThreadedMode = tc.mode
			results, err := d.PrepareResourceClaims(ctx, tc.claims)
			require.NoError(t, err)
			for uid, result := range results {
				require.NoError(t, result.Err, "claim %s", uid)
			}
			if tc.expiring {
				ctx = &expiringContext{Context: ctx}
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
			umask := syscall.Umask(0o077)
			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: vmID, Name: "app", PodSandboxId: pod.Id, Env: tc.env})
			syscall.Umask(umask)
			dir := filepath.Join(d.kubeletRootDir, "plugins", testDriverName, "cgroup-links", vmID)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.NoDirExists(t, dir, "a refused container leaves no directory")
				_, bound := d.allocationState.Owner("vm")
				require.False(t, bound, "a refused container leaves the claim unbound")
				return
			}
			require.NoError(t, err)
			if !tc.wantMount {
				require.Empty(t, adjust.GetMounts())
				require.NoDirExists(t, dir)
				return
			}
			require.Equal(t, []*api.Mount{{Destination: "/var/run/dracpu", Type: "bind", Source: dir, Options: []string{"rbind", "ro"}}}, adjust.GetMounts())
			info, err := os.Stat(dir)
			require.NoError(t, err)
			require.Equal(t, fs.FileMode(0o755), info.Mode().Perm(), "the container's user crosses it")
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "the link is written when the container starts")
		})
	}
}

// StartContainer points /var/run/dracpu/cgroup at the thread root as the
// container resolves it, before it moves the process.
func TestStartContainerLinksTheThreadRoot(t *testing.T) {
	const root = "kubepods/burstable/podvm/vm"
	init := fmt.Sprintf("%d\n", vmInit)
	for _, tc := range []struct {
		name        string
		namespaces  []*api.LinuxNamespace
		annotations map[string]string
		procs       map[string]string
		wantLink    string
	}{{
		name:       "a cgroup namespace of its own",
		namespaces: ownCgroupNamespace(),
		procs:      map[string]string{root: init},
		wantLink:   "/sys/fs/cgroup",
	}, {
		name:        "crun's delegate cgroup, holding the init",
		namespaces:  ownCgroupNamespace(),
		annotations: map[string]string{"run.oci.delegate-cgroup": "a/b"},
		procs:       map[string]string{root: "", root + "/a": "", root + "/a/b": init},
		wantLink:    "/sys/fs/cgroup/a/b",
	}, {
		name:        "a delegate cgroup written with empty and dot components",
		namespaces:  ownCgroupNamespace(),
		annotations: map[string]string{"run.oci.delegate-cgroup": "/a//./b/"},
		procs:       map[string]string{root: "", root + "/a": "", root + "/a/b": init},
		wantLink:    "/sys/fs/cgroup/a/b",
	}, {
		name:        "a delegate cgroup the thread root does not end with",
		namespaces:  ownCgroupNamespace(),
		annotations: map[string]string{"run.oci.delegate-cgroup": "x"},
		procs:       map[string]string{root: init},
		wantLink:    "/sys/fs/cgroup",
	}, {
		name:        "a delegate cgroup matching only part of a component",
		namespaces:  ownCgroupNamespace(),
		annotations: map[string]string{"run.oci.delegate-cgroup": "b"},
		procs:       map[string]string{root: "", root + "/ab": init},
		wantLink:    "/sys/fs/cgroup",
	}, {
		name:     "the host's cgroup namespace",
		procs:    map[string]string{root: init},
		wantLink: "/sys/fs/cgroup/" + root,
	}, {
		name:       "another container's cgroup namespace",
		namespaces: []*api.LinuxNamespace{{Type: "cgroup", Path: "/proc/77/ns/cgroup"}},
		procs:      map[string]string{root: init},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			tree := newWritableTree(t, tc.procs)
			d, pod := linkDriver(t, "place", vmSplit, tree, nil)
			dir := filepath.Join(d.kubeletRootDir, "plugins", testDriverName, "cgroup-links", vmID)
			mover := &linkCheckingMover{t: t, link: filepath.Join(dir, "cgroup"), want: tc.wantLink}
			d.hostCgroups = mover
			ctr := &api.Container{Id: vmID, Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"),
				Annotations: tc.annotations, Linux: &api.LinuxContainer{CgroupsPath: "/" + root, Namespaces: tc.namespaces}}
			_, _, err := d.CreateContainer(ctx, pod, ctr)
			require.NoError(t, err)
			require.NoError(t, d.StartContainer(ctx, pod, ctr))
			require.Len(t, mover.moves, 1, "the process is moved")
			link, err := os.Readlink(filepath.Join(dir, "cgroup"))
			if tc.wantLink == "" {
				require.ErrorIs(t, err, fs.ErrNotExist, "no link")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantLink, link)
		})
	}
}

// StartContainer builds no groups for a threaded container the driver did not
// see created, whatever rule its claims break, and moves no process: the
// container runs on its claims' CPUs without its groups and their limits.
// CreateContainer makes the link directory last, so its absence marks it.
func TestStartContainerBuildsNothingForAContainerCreatedUnseen(t *testing.T) {
	const root = "kubepods/burstable/podvm/vm"
	sameGroup := func(uid types.UID) *resourceapi.ResourceClaim {
		return withConfig(claimWithResults(uid, claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`))
	}
	for _, tc := range []struct {
		name      string
		claims    []*resourceapi.ResourceClaim
		env       []string
		created   bool
		wantBuilt bool
	}{{
		name:      "a container the driver saw created",
		claims:    []*resourceapi.ResourceClaim{vmClaim("vm", vmSplit)},
		env:       claimEnv("vm", "vcpu", "vmm-cpu"),
		created:   true,
		wantBuilt: true,
	}, {
		name:   "a container created while the driver was away",
		claims: []*resourceapi.ResourceClaim{vmClaim("vm", vmSplit)},
		env:    claimEnv("vm", "vcpu", "vmm-cpu"),
	}, {
		name:   "a container created while the driver was away whose claims give two groups one name",
		claims: []*resourceapi.ResourceClaim{sameGroup("a"), sameGroup("b")},
		env:    append(claimEnv("a", "vcpu"), claimEnv("b", "vcpu")...),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			tree := newWritableTree(t, map[string]string{root: fmt.Sprintf("%d\n", vmInit)})
			reg := prometheus.NewRegistry()
			d := sharingDriver(t, true, nil, cpumetrics.New(reg))
			d.cgroupThreadedMode = "place"
			d.cgroups = tree
			d.ownCgroupFound = true
			d.eventRecorder = &capturingRecorder{}
			d.providers.Processes = fakeProcesses{status: map[int]string{vmInit: vmInitStatus}}
			mover := &fakeMover{}
			d.hostCgroups = mover
			results, err := d.PrepareResourceClaims(ctx, tc.claims)
			require.NoError(t, err)
			for uid, result := range results {
				require.NoError(t, result.Err, "claim %s", uid)
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
			ctr := &api.Container{Id: vmID, Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: tc.env,
				Linux: &api.LinuxContainer{CgroupsPath: "/" + root, Namespaces: ownCgroupNamespace()}}
			if tc.created {
				_, _, err := d.CreateContainer(ctx, pod, ctr)
				require.NoError(t, err)
			}
			require.NoError(t, d.StartContainer(ctx, pod, ctr))
			_, built := tree.MapFS[path.Join(root, "vcpu")]
			require.Equal(t, tc.wantBuilt, built, "the group vcpu")
			if tc.wantBuilt {
				require.Len(t, mover.moves, 1, "the process is moved")
				return
			}
			require.Empty(t, tree.read(t, path.Join(root, "cgroup.subtree_control")))
			require.Empty(t, mover.moves, "the process is not moved")
			require.Empty(t, d.podState.ThreadedContainers())
			families, err := reg.Gather()
			require.NoError(t, err)
			for _, family := range families {
				if family.GetName() != "dra_cpu_threaded_containers_total" {
					continue
				}
				for _, metric := range family.Metric {
					require.Zero(t, metricSampleValue(metric), "a container given no groups is counted under %v", metric.Label)
				}
			}
		})
	}
}

// linkCheckingMover checks, when it moves a process, that the link is
// already written.
type linkCheckingMover struct {
	fakeMover
	t    *testing.T
	link string
	want string
}

func (m *linkCheckingMover) MoveProcess(fsys cgroupfs.FS, dir string, pid int) error {
	link, err := os.Readlink(m.link)
	if m.want == "" {
		require.ErrorIs(m.t, err, fs.ErrNotExist)
	} else {
		require.NoError(m.t, err, "the link is written before the move")
		require.Equal(m.t, m.want, link)
	}
	return m.fakeMover.MoveProcess(fsys, dir, pid)
}

// RemoveContainer removes a container's link directory by its id, after
// StopContainer has dropped what the driver knew of it, a missing one counting
// as removed, and does so before the node's profile is read too.
func TestRemoveContainerRemovesTheLinkDirectory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window bool
	}{
		{name: "once configured"},
		{name: "before the node's profile is read", window: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			tree := newWritableTree(t, map[string]string{"kubepods/pod/vm": fmt.Sprintf("%d\n", vmInit)})
			d, pod := linkDriver(t, "create", `{"enabled": true}`, tree, nil)
			ctr := &api.Container{Id: vmID, Name: "compute", PodSandboxId: pod.Id, Env: claimEnv("vm", "vcpu", "vmm-cpu")}
			_, _, err := d.CreateContainer(ctx, pod, ctr)
			require.NoError(t, err)
			dir := filepath.Join(d.kubeletRootDir, "plugins", testDriverName, "cgroup-links", vmID)
			require.DirExists(t, dir)
			_, err = d.StopContainer(ctx, pod, ctr)
			require.NoError(t, err)
			if tc.window {
				d.window = newUnconfiguredWindow()
			}
			require.NoError(t, d.RemoveContainer(ctx, pod, ctr))
			require.NoDirExists(t, dir)
			require.NoError(t, d.RemoveContainer(ctx, pod, ctr), "a missing directory counts as removed")
		})
	}
}

// Every Synchronize removes the link directory of each container it does not
// list, keeps the others, follows no link and leaves other names alone, and
// repairs each running threaded container's link against the thread root it
// finds, keeping the delegate cgroup after the process moved. A failure fails
// nothing.
func TestSynchronizeSweepsAndRepairsLinks(t *testing.T) {
	const root = "kubepods/burstable/podvm/vm"
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	tree := newWritableTree(t, map[string]string{root: "", root + "/a": "", root + "/a/b": fmt.Sprintf("%d\n", vmInit)})
	reg := prometheus.NewRegistry()
	d, pod := linkDriver(t, "place", vmSplit, tree, reg)
	vm := &api.Container{Id: vmID, Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"),
		Annotations: map[string]string{"run.oci.delegate-cgroup": "a/b"}, State: api.ContainerState_CONTAINER_RUNNING,
		Linux: &api.LinuxContainer{CgroupsPath: "/" + root, Namespaces: ownCgroupNamespace(), Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-5"}}}}
	_, _, err := d.CreateContainer(ctx, pod, vm)
	require.NoError(t, err)
	require.NoError(t, d.StartContainer(ctx, pod, vm))
	require.Equal(t, "4242", tree.read(t, path.Join(root, "a/b/vmm-cpu/cgroup.procs")), "the process is in its place group")

	links := filepath.Join(d.kubeletRootDir, "plugins", testDriverName, "cgroup-links")
	link := filepath.Join(links, vmID, "cgroup")
	require.NoError(t, os.Remove(link))
	require.NoError(t, os.Symlink("/sys/fs/cgroup/wrong", link))
	for _, dir := range []string{otherID, goneID, "not-a-container"} {
		require.NoError(t, os.MkdirAll(filepath.Join(links, dir), 0o755))
	}
	elsewhere := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(elsewhere, "kept"), nil, 0o600))
	linkedID := strings.Repeat("d", 64)
	require.NoError(t, os.Symlink(elsewhere, filepath.Join(links, linkedID)))
	created := &api.Container{Id: otherID, Name: "sidecar", PodSandboxId: pod.Id, State: api.ContainerState_CONTAINER_CREATED}

	_, err = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{vm, created})
	require.NoError(t, err)
	target, err := os.Readlink(link)
	require.NoError(t, err)
	require.Equal(t, "/sys/fs/cgroup/a/b", target, "a wrong link is replaced, the delegate cgroup kept")
	require.DirExists(t, filepath.Join(links, otherID), "a listed container's directory is kept, created or not")
	require.NoDirExists(t, filepath.Join(links, goneID), "an unlisted container's directory is removed")
	require.DirExists(t, filepath.Join(links, "not-a-container"), "a name that is no container id is left")
	require.FileExists(t, filepath.Join(elsewhere, "kept"), "no link is followed")
	_, err = os.Lstat(filepath.Join(links, linkedID))
	require.NoError(t, err, "a link is left")
	require.Equal(t, 0.0, counterOrZero(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": "place", "result": "link_error"}))

	if os.Geteuid() == 0 {
		t.Skip("root removes what its mode keeps it from")
	}
	stuck := filepath.Join(links, goneID)
	require.NoError(t, os.MkdirAll(filepath.Join(stuck, "inner"), 0o755))
	require.NoError(t, os.Chmod(stuck, 0o500))
	t.Cleanup(func() { _ = os.Chmod(stuck, 0o755) })
	_, err = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{vm})
	require.NoError(t, err, "a failed removal never fails Synchronize")
	require.DirExists(t, stuck)
	require.Equal(t, 1.0, counterOrZero(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": "place", "result": "link_error"}))
	require.NoDirExists(t, filepath.Join(links, otherID), "the next Synchronize removes what it no longer lists")
}

// The Synchronize that keeps the runtime's containers until the node's
// profile is read sweeps the link directories as every Synchronize does.
func TestSynchronizeSweepsLinksBeforeTheProfileIsRead(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	d, pod := linkDriver(t, "create", `{"enabled": true}`, newWritableTree(t, nil), nil)
	links := filepath.Join(d.kubeletRootDir, "plugins", testDriverName, "cgroup-links")
	for _, dir := range []string{vmID, goneID} {
		require.NoError(t, os.MkdirAll(filepath.Join(links, dir), 0o755))
	}
	d.window = newUnconfiguredWindow()
	_, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{{Id: vmID, Name: "compute", PodSandboxId: pod.Id}})
	require.NoError(t, err)
	require.DirExists(t, filepath.Join(links, vmID))
	require.NoDirExists(t, filepath.Join(links, goneID))
}
