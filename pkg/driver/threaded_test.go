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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/admission"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	devattr "github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
)

func threadedParams(block string) string {
	return `{"apiVersion": "v1alpha1", "cgroupConfig": {"threaded": ` + block + `}}`
}

const vmSplit = `{"enabled": true, "place": "vmm-cpu", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "vmm-cpu", "requests": ["vmm-cpu"]}]}`

// vmClaim is the fleet's VM: two exclusive CPUs on the vms partition and a
// 250m share of the default partition, split as block says.
func vmClaim(uid types.UID, block string) *resourceapi.ResourceClaim {
	return withConfig(claimWithResults(uid,
		claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
		claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, threadedParams(block))
}

// statusGroups returns the group each status entry of a claim names, by
// device, "" for an entry naming none.
func statusGroups(t *testing.T, d *CPUDriver, claim *resourceapi.ResourceClaim) map[string]string {
	t.Helper()
	entries, err := d.claimStatus(testr.New(t), claim)
	require.NoError(t, err)
	groups := map[string]string{}
	for _, entry := range entries {
		var data v1alpha1.ClaimPlacementStatus
		require.NoError(t, json.Unmarshal(entry.Data.Raw, &data))
		groups[entry.Device] = data.Group
	}
	return groups
}

// metadataGroups returns the dra.cpu/group each prepared device's metadata
// names, by device, "" for none.
func metadataGroups(devices []kubeletplugin.Device) map[string]string {
	groups := map[string]string{}
	for _, dev := range devices {
		groups[dev.DeviceName] = ""
		if attr, ok := dev.Metadata.Attributes[string(devattr.AttributeGroup)]; ok {
			groups[dev.DeviceName] = *attr.StringValue
		}
	}
	return groups
}

// ownedAlternativePicked is vcpu beside a firstAvailable request helper whose
// owned alternative "other" the scheduler picked over its shared one.
func ownedAlternativePicked() *resourceapi.ResourceClaim {
	claim := claimWithResults("vm",
		claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
		claimResult{request: "helper/shared", written: "500m", consumed: "500m", device: "cpudevnuma000"})
	picked := &claim.Status.Allocation.Devices.Results[1]
	picked.Request, picked.Device = "helper/other", "cpudevnuma000-vms"
	picked.ConsumedCapacity = map[resourceapi.QualifiedName]resource.Quantity{devattr.CPUResourceQualifiedName: resource.MustParse("1")}
	return claim
}

// chartKinds are the kinds of the classes a chart with sharingDriver's
// partitions renders.
var chartKinds = admission.ClassKinds("dra.cpu", driverconfig.Config{CPUPartitions: []driverconfig.CPUPartition{
	{Name: devattr.DefaultPartitionName}, {Name: "vms", Role: devattr.PARTITION_ROLE_EXCLUSIVE}}})

// mixedClaim holds vcpu and a firstAvailable request helper whose alternatives
// are this driver's cpu and another driver's gpu, the gpu one allocated.
func mixedClaim(block string) *resourceapi.ResourceClaim {
	claim := &resourceapi.ResourceClaim{ObjectMeta: metav1.ObjectMeta{UID: "mixed", Name: "mixed", Namespace: "vms"}}
	claim.Spec.Devices.Requests = []resourceapi.DeviceRequest{
		{Name: "vcpu", Exactly: &resourceapi.ExactDeviceRequest{DeviceClassName: "dra.cpu.vms"}},
		{Name: "helper", FirstAvailable: []resourceapi.DeviceSubRequest{{Name: "cpu", DeviceClassName: "dra.cpu"}, {Name: "gpu", DeviceClassName: "gpu.example.com"}}},
	}
	claim.Status.Allocation = &resourceapi.AllocationResult{}
	claim.Status.Allocation.Devices.Results = []resourceapi.DeviceRequestAllocationResult{
		{Driver: testDriverName, Pool: testNodeName, Device: "cpudevnuma000-vms", Request: "vcpu",
			ConsumedCapacity: map[resourceapi.QualifiedName]resource.Quantity{devattr.CPUResourceQualifiedName: resource.MustParse("2")}},
		{Driver: "gpu.example.com", Pool: testNodeName, Device: "gpu0", Request: "helper/gpu"},
	}
	return withConfig(claim, resourceapi.AllocationConfigSourceClaim, nil, threadedParams(block))
}

// Prepare records the group each request's CPUs land in, a group no result
// reaches being none, and writes it to each status entry and metadata file of
// a threaded claim, after judging the block and the node's mode.
func TestPrepareThreadedClaims(t *testing.T) {
	for _, tc := range []struct {
		name         string
		mode         string
		classKinds   map[string]admission.Kind
		claim        *resourceapi.ResourceClaim
		wantErr      string
		wantLayout   *store.Layout
		wantGroups   map[string]string
		wantByDevice map[string]string
	}{{
		name:         "the VM's split",
		mode:         "place",
		claim:        vmClaim("vm", vmSplit),
		wantLayout:   &store.Layout{Place: "vmm-cpu"},
		wantGroups:   map[string]string{"vcpu": "vcpu", "vmm-cpu": "vmm-cpu"},
		wantByDevice: map[string]string{"cpudevnuma000-vms": "vcpu", "cpudevnuma000": "vmm-cpu"},
	}, {
		name:         "one group per request without a groups list",
		mode:         "create",
		claim:        vmClaim("vm", `{"enabled": true}`),
		wantLayout:   &store.Layout{},
		wantGroups:   map[string]string{"vcpu": "vcpu", "vmm-cpu": "vmm-cpu"},
		wantByDevice: map[string]string{"cpudevnuma000-vms": "vcpu", "cpudevnuma000": "vmm-cpu"},
	}, {
		name: "two requests in one group",
		mode: "create",
		claim: withConfig(claimWithResults("vm",
			claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"},
			claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "groups": [{"name": "all", "requests": ["vcpu", "io"]}]}`)),
		wantLayout:   &store.Layout{},
		wantGroups:   map[string]string{"vcpu": "all", "io": "all"},
		wantByDevice: map[string]string{"cpudevnuma000-vms": "all", "cpudevnuma000": "all"},
	}, {
		name: "WB26: alternatives split across groups, the shared one picked",
		mode: "place",
		claim: withConfig(claimWithResults("vm",
			claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
			claimResult{request: "helper/shared", written: "500m", consumed: "500m", device: "cpudevnuma000"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu", "helper/other"]}, {"name": "helper", "requests": ["helper/shared"]}]}`)),
		wantLayout:   &store.Layout{},
		wantGroups:   map[string]string{"vcpu": "vcpu", "helper": "helper"},
		wantByDevice: map[string]string{"cpudevnuma000-vms": "vcpu", "cpudevnuma000": "helper"},
	}, {
		name: "WB26: alternatives split across groups, the owned one picked, its group left empty",
		mode: "place",
		claim: withConfig(ownedAlternativePicked(),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "groups": [{"name": "vcpu", "requests": ["vcpu", "helper/other"]}, {"name": "helper", "requests": ["helper/shared"]}]}`)),
		wantLayout:   &store.Layout{},
		wantGroups:   map[string]string{"vcpu": "vcpu", "helper": "vcpu"},
		wantByDevice: map[string]string{"cpudevnuma000-vms": "vcpu"},
	}, {
		name: "a share's entry names its partition and group alone",
		mode: "create",
		claim: withConfig(claimWithResults("vm",
			claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
		wantLayout:   &store.Layout{},
		wantGroups:   map[string]string{"io": "io"},
		wantByDevice: map[string]string{"cpudevnuma000": "io"},
	}, {
		name: "an unthreaded claim names no group",
		mode: "place",
		claim: claimWithResults("vm",
			claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}),
		wantGroups:   map[string]string{"vcpu": ""},
		wantByDevice: map[string]string{"cpudevnuma000-vms": ""},
	}, {
		name:         "WB9: an inert block naming place on a create node",
		mode:         "create",
		claim:        withConfig(claimWithResults("vm", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}), resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"place": "vcpu"}`)),
		wantGroups:   map[string]string{"vcpu": ""},
		wantByDevice: map[string]string{"cpudevnuma000-vms": ""},
	}, {
		name:    "WC12: enabled on a node running false",
		claim:   vmClaim("vm", vmSplit),
		wantErr: "node \"test-node\" runs `cgroupThreadedMode: false`; threaded cgroups need create or place",
	}, {
		name:    "WC12: place on a node running create",
		mode:    "create",
		claim:   vmClaim("vm", vmSplit),
		wantErr: "cgroupConfig.threaded.place \"vmm-cpu\": node \"test-node\" runs `cgroupThreadedMode: create`, which moves no process; drop place",
	}, {
		name:       "WB30: place on a group that received no result",
		mode:       "place",
		classKinds: chartKinds,
		claim:      mixedClaim(`{"enabled": true, "place": "helper", "groups": [{"name": "vcpu", "requests": ["vcpu"]}, {"name": "helper", "requests": ["helper"]}]}`),
		wantErr:    `cgroupConfig.threaded.place "helper": group "helper" received no result on this allocation and is not built; place a group that names a request`,
	}, {
		name: "WC10: a DeviceClass giving cgroupConfig",
		mode: "place",
		claim: withConfig(claimWithResults("vm", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"}),
			resourceapi.AllocationConfigSourceClass, nil, threadedParams(`{"enabled": true}`)),
		wantErr: `DeviceClass "dra.cpu" sets cgroupConfig; set it in the claim`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, true, nil, nil)
			d.cgroupThreadedMode = tc.mode
			d.classKinds = tc.classKinds
			results, err := d.PrepareResourceClaims(context.Background(), []*resourceapi.ResourceClaim{tc.claim})
			require.NoError(t, err)
			result := results[tc.claim.UID]
			if tc.wantErr != "" {
				require.EqualError(t, result.Err, tc.wantErr)
				_, recorded := d.allocationState.GetClaimRecord(tc.claim.UID)
				require.False(t, recorded, "a refused claim leaves no record")
				return
			}
			require.NoError(t, result.Err)
			record, ok := d.allocationState.GetClaimRecord(tc.claim.UID)
			require.True(t, ok)
			require.Equal(t, tc.wantLayout, record.Layout)
			groups := map[string]string{}
			for _, request := range record.Requests {
				groups[request.Request] = request.Group
			}
			require.Equal(t, tc.wantGroups, groups)
			require.Equal(t, tc.wantByDevice, metadataGroups(result.Devices), "each metadata file names its request's group")
			require.Equal(t, tc.wantByDevice, statusGroups(t, d, tc.claim), "each status entry names its result's group")
			written, err := d.cdiMgr.GetDeviceAllocations(tc.claim.UID)
			require.NoError(t, err)
			require.Equal(t, record, written.storeRecord(), "the CDI record keeps the layout")
		})
	}
}

// writableTree is a cgroup2 tree in memory: mkdir makes a cgroup's files, a
// child turned threaded makes its parent a thread root (cgroup-v2.rst:377-379),
// a write to a file the tree lacks fails, and chown records the owner.
type writableTree struct {
	fstest.MapFS
	owners map[string]cgroupfs.Owner
}

func newWritableTree(t *testing.T, procs map[string]string) *writableTree {
	t.Helper()
	tree := &writableTree{MapFS: fstest.MapFS{}, owners: map[string]cgroupfs.Owner{}}
	for dir, pids := range procs {
		var parent string
		for _, part := range strings.Split(dir, "/") {
			parent = path.Join(parent, part)
			if err := tree.Mkdir(parent, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
				t.Fatal(err)
			}
		}
		tree.MapFS[path.Join(dir, "cgroup.procs")].Data = []byte(pids)
	}
	return tree
}

func (w *writableTree) Mkdir(name string, perm fs.FileMode) error {
	if _, ok := w.MapFS[name]; ok {
		return &fs.PathError{Op: "mkdir", Path: name, Err: fs.ErrExist}
	}
	w.MapFS[name] = &fstest.MapFile{Mode: fs.ModeDir | perm}
	for _, file := range []string{"cgroup.procs", "cgroup.threads", "cgroup.subtree_control", "cpuset.cpus", "cpu.max", "cpu.weight"} {
		w.MapFS[path.Join(name, file)] = &fstest.MapFile{}
	}
	w.MapFS[path.Join(name, "cgroup.type")] = &fstest.MapFile{Data: []byte("domain")}
	return nil
}

func (w *writableTree) WriteFile(name string, data []byte) error {
	file, ok := w.MapFS[name]
	if !ok {
		return &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	file.Data = slices.Clone(data)
	if path.Base(name) == "cgroup.type" && string(data) == "threaded" {
		if parent, ok := w.MapFS[path.Join(path.Dir(path.Dir(name)), "cgroup.type")]; ok && string(parent.Data) == "domain" {
			parent.Data = []byte("domain threaded")
		}
	}
	return nil
}

func (w *writableTree) Chown(name string, uid, gid int) error {
	if _, ok := w.MapFS[name]; !ok {
		return &fs.PathError{Op: "chown", Path: name, Err: fs.ErrNotExist}
	}
	w.owners[name] = cgroupfs.Owner{UID: uid, GID: gid}
	return nil
}

func (w *writableTree) read(t *testing.T, name string) string {
	t.Helper()
	file, ok := w.MapFS[name]
	require.Truef(t, ok, "%s exists", name)
	return string(file.Data)
}

// fakeProcesses are other containers' processes: the status and cgroup files
// of each pid.
type fakeProcesses struct {
	status map[int]string
	cgroup map[int]string
}

func (f fakeProcesses) Status(pid int) ([]byte, error) {
	status, ok := f.status[pid]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/status", pid), Err: syscall.ENOENT}
	}
	return []byte(status), nil
}

func (f fakeProcesses) Cgroup(pid int) ([]byte, error) {
	cgroup, ok := f.cgroup[pid]
	if !ok {
		return nil, &fs.PathError{Op: "open", Path: fmt.Sprintf("/proc/%d/cgroup", pid), Err: syscall.ENOENT}
	}
	return []byte(cgroup), nil
}

const (
	vmInit = 4242
	// The status of a process in a user namespace as the host reads it: the
	// fourth field of each line, the filesystem id, is the one the kernel
	// checks a file's owner against.
	vmInitStatus = "Name:\tcloud-hypervisor\nUid:\t2038693884\t2038693884\t2038693884\t2038693885\nGid:\t2038693884\t2038693884\t2038693884\t2038693886\n"
)

func claimEnv(claim types.UID, requests ...string) []string {
	env := make([]string, 0, len(requests))
	for _, request := range requests {
		env = append(env, fmt.Sprintf("%s_%s_%s=%s", cdiEnvVarPrefix, claim, request, cdiEnvDynamicValue))
	}
	return env
}

// threadedDriver is sharingDriver under mode, the VM claim prepared, its
// container created, and its init's cgroup in tree.
func threadedDriver(t *testing.T, mode string, tree *writableTree, reg *prometheus.Registry) (*CPUDriver, *api.PodSandbox) {
	t.Helper()
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	var recorder Recorder
	if reg != nil {
		recorder = cpumetrics.New(reg)
	}
	d := sharingDriver(t, true, nil, recorder)
	d.cgroupThreadedMode = mode
	d.cgroups = tree
	d.providers.Processes = fakeProcesses{
		status: map[int]string{vmInit: vmInitStatus},
		cgroup: map[int]string{vmInit: "0::/kubepods/elsewhere\n"},
	}
	block := vmSplit
	if mode == cgroupfs.ThreadedModeCreate {
		block = `{"enabled": true}`
	}
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{vmClaim("vm", block)})
	require.NoError(t, err)
	require.NoError(t, results["vm"].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
	_, _, err = d.CreateContainer(ctx, pod, &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Env: claimEnv("vm", "vcpu", "vmm-cpu")})
	require.NoError(t, err)
	return d, pod
}

// StartContainer builds a threaded container's groups under its thread root,
// the cgroup of its cgroup's subtree that holds its init, whatever the
// runtime names it, and refuses one whose init sits outside that subtree.
func TestStartContainerBuildsThreadedGroups(t *testing.T) {
	const (
		runcCgroup  = "kubepods/burstable/podvm/vm"
		systemdPath = "kubepods-burstable-podvm.slice:cri-containerd:vm"
		crunScope   = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podvm.slice/cri-containerd-vm.scope"
	)
	init := fmt.Sprintf("%d\n", vmInit)
	for _, tc := range []struct {
		name        string
		mode        string
		cgroupsPath string
		procs       map[string]string
		wantRoot    string
		wantErr     string
	}{{
		name:        "runc on the cgroupfs driver: the container's cgroup",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: init},
		wantRoot:    runcCgroup,
	}, {
		name:        "crun on the systemd driver: its sub-cgroup container",
		mode:        "create",
		cgroupsPath: systemdPath,
		procs:       map[string]string{crunScope: "", crunScope + "/container": init},
		wantRoot:    crunScope + "/container",
	}, {
		name:        "crun's run.oci.delegate-cgroup: a nested sub-cgroup",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: "", runcCgroup + "/a": "", runcCgroup + "/a/b": init},
		wantRoot:    runcCgroup + "/a/b",
	}, {
		name:        "WC13: an init outside the container's cgroup",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: "", "kubepods/elsewhere": init},
		wantErr:     `container "compute": init runs in "/kubepods/elsewhere", outside the container's cgroup "/kubepods/burstable/podvm/vm", so threaded groups cannot be built`,
	}, {
		name:        "a container's cgroup the tree does not show",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{"kubepods/elsewhere": init},
		wantErr:     `container "compute": find pid 4242 below kubepods/burstable/podvm/vm: open kubepods/burstable/podvm/vm: file does not exist`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tree := newWritableTree(t, tc.procs)
			reg := prometheus.NewRegistry()
			d, pod := threadedDriver(t, tc.mode, tree, reg)
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			ctr := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit,
				Env: claimEnv("vm", "vcpu", "vmm-cpu"), Linux: &api.LinuxContainer{CgroupsPath: tc.cgroupsPath}}
			err := d.StartContainer(ctx, pod, ctr)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": tc.mode, "result": "error"}))
				require.Empty(t, d.podState.ThreadedContainers(), "no thread root is recorded")
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": tc.mode, "result": "success"}))
			root := tc.wantRoot
			require.Equal(t, "+cpu +cpuset", tree.read(t, path.Join(root, "cgroup.subtree_control")))
			for _, want := range []struct{ group, cpus, max, weight string }{
				{group: "vcpu", cpus: "1,5", max: "max 100000", weight: "1"},
				{group: "vmm-cpu", cpus: "0,2-4", max: "25000 100000", weight: "10000"},
			} {
				child := path.Join(root, want.group)
				require.Equal(t, "threaded", tree.read(t, path.Join(child, "cgroup.type")), want.group)
				require.Equal(t, want.cpus, tree.read(t, path.Join(child, "cpuset.cpus")), want.group)
				require.Equal(t, want.max, tree.read(t, path.Join(child, "cpu.max")), want.group)
				require.Equal(t, want.weight, tree.read(t, path.Join(child, "cpu.weight")), want.group)
				owner := cgroupfs.Owner{UID: 2038693885, GID: 2038693886}
				require.Equal(t, owner, tree.owners[path.Join(child, "cgroup.threads")], want.group)
				require.Equal(t, owner, tree.owners[path.Join(child, "cgroup.procs")], want.group)
			}
			require.Equal(t, cgroupfs.Owner{UID: 2038693885, GID: 2038693886}, tree.owners[path.Join(root, "cgroup.procs")])
			_, threadsHandedOver := tree.owners[path.Join(root, "cgroup.threads")]
			require.False(t, threadsHandedOver, "the thread root's cgroup.threads stays root's")
			require.Equal(t, init, tree.read(t, path.Join(root, "cgroup.procs")), "the process is not moved")
			require.Equal(t, map[types.UID]store.ThreadedContainer{"vm": {ThreadRoot: root, ClaimRequests: []store.ClaimRequestRef{
				{ClaimUID: "vm", Request: "vcpu"}, {ClaimUID: "vm", Request: "vmm-cpu"}}}}, d.podState.ThreadedContainers())
		})
	}
}

// A container with no threaded claim, or on a node running false, gets no
// groups and reads no cgroup.
func TestStartContainerBuildsNothingWithoutALayout(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	for _, tc := range []struct {
		name  string
		mode  string
		block string
	}{
		{name: "an unthreaded claim", mode: "place"},
		{name: "a threaded claim recorded before the node's mode turned false", block: `{"enabled": true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := sharingDriver(t, true, nil, nil)
			d.cgroupThreadedMode = "create"
			claim := claimWithResults("vm", claimResult{request: "vcpu", written: "2", consumed: "2", device: "cpudevnuma000-vms"})
			if tc.block != "" {
				claim = withConfig(claim, resourceapi.AllocationConfigSourceClaim, nil, threadedParams(tc.block))
			}
			results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{claim})
			require.NoError(t, err)
			require.NoError(t, results["vm"].Err)
			d.cgroupThreadedMode = tc.mode
			d.cgroups = readTree{fstest.MapFS{}}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
			ctr := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu"),
				Linux: &api.LinuxContainer{CgroupsPath: "/kubepods/pod/vm"}}
			_, _, err = d.CreateContainer(ctx, pod, ctr)
			require.NoError(t, err)
			require.NoError(t, d.StartContainer(ctx, pod, ctr))
			require.Empty(t, d.podState.ThreadedContainers())
		})
	}
}

// A threaded container refuses a claim of this driver that is not threaded
// and two threaded claims giving one group name; its caps go to its groups,
// so it is never refused or given a quota for a capped share beside another
// request, and a kubelet quota on it is warned about.
func TestCreateContainerJudgesThreadedClaims(t *testing.T) {
	split := func(uid types.UID, block string) *resourceapi.ResourceClaim {
		return withConfig(claimWithResults(uid,
			claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"},
			claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(block))
	}
	for _, tc := range []struct {
		name       string
		claims     []*resourceapi.ResourceClaim
		env        []string
		linux      *api.LinuxContainer
		wantErr    string
		wantEvents []string
	}{{
		name:   "a capped share beside exclusive CPUs, each in its group",
		claims: []*resourceapi.ResourceClaim{split("vm", `{"enabled": true}`)},
		env:    claimEnv("vm", "vcpu", "vmm-cpu"),
	}, {
		name: "a threaded claim holding one capped share alone",
		claims: []*resourceapi.ResourceClaim{withConfig(claimWithResults("solo",
			claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`))},
		env: claimEnv("solo", "vmm-cpu"),
	}, {
		name:   "a kubelet quota on a threaded container",
		claims: []*resourceapi.ResourceClaim{split("vm", `{"enabled": true}`)},
		env:    claimEnv("vm", "vcpu", "vmm-cpu"),
		linux:  kubeletQuota(50000),
		wantEvents: []string{
			`Warning CPULimitReplacesCap container "app": its CPU limit of 500m replaces the limit of 250m of request "vmm-cpu" of claim "vm"`,
		},
	}, {
		name: "WC3: a claim that is not threaded beside a threaded one",
		claims: []*resourceapi.ResourceClaim{split("vm", `{"enabled": true}`),
			claimWithResults("extra", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"})},
		env:     append(claimEnv("vm", "vcpu", "vmm-cpu"), claimEnv("extra", "io")...),
		wantErr: `container "app": claim "extra" is not threaded but shares the container with threaded claim "vm"; move its requests into the threaded claim or another container`,
	}, {
		name: "two threaded claims giving one group name",
		claims: []*resourceapi.ResourceClaim{
			withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
			withConfig(claimWithResults("b", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
		},
		env:     append(claimEnv("a", "vcpu"), claimEnv("b", "vcpu")...),
		wantErr: `container "app": claims "a" and "b" both have group "vcpu"; give the groups different names`,
	}, {
		name: "WC4: two threaded claims that both set place",
		claims: []*resourceapi.ResourceClaim{
			withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "place": "vcpu"}`)),
			withConfig(claimWithResults("b", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "place": "io"}`)),
		},
		env:     append(claimEnv("a", "vcpu"), claimEnv("b", "io")...),
		wantErr: `container "app": claims "a" and "b" both set place; keep place in one claim`,
	}, {
		name: "two threaded claims whose groups differ",
		claims: []*resourceapi.ResourceClaim{
			withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
			withConfig(claimWithResults("b", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
		},
		env: append(claimEnv("a", "vcpu"), claimEnv("b", "io")...),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			d := sharingDriver(t, true, nil, nil)
			d.cgroupThreadedMode = "place"
			events := &capturingRecorder{}
			d.eventRecorder = events
			results, err := d.PrepareResourceClaims(ctx, tc.claims)
			require.NoError(t, err)
			for uid, result := range results {
				require.NoError(t, result.Err, "claim %s", uid)
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
			adjust, _, err := d.CreateContainer(ctx, pod, &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: tc.env, Linux: tc.linux})
			require.Equal(t, tc.wantEvents, events.lines())
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				for _, claim := range tc.claims {
					_, bound := d.allocationState.Owner(claim.UID)
					require.Falsef(t, bound, "claim %s is refused before it binds", claim.UID)
				}
				return
			}
			require.NoError(t, err)
			require.Nil(t, adjust.GetLinux().GetResources().GetCpu().GetQuota(), "the container's cgroup gets no quota")
		})
	}
}

// When an exclusive request takes CPUs of the default partition, each running
// threaded group holding a share there is rewritten with the take's
// CreateContainer, and widened again with the give-back's push; a group that
// is gone is logged and counted, and fails nothing.
func TestThreadedSharesFollowTheDefaultPartition(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	const root = "kubepods/burstable/podvm/vm"
	tree := newWritableTree(t, map[string]string{root: fmt.Sprintf("%d\n", vmInit)})
	reg := prometheus.NewRegistry()
	d := allowingDriver(t, true, false, nil)
	d.metrics = cpumetrics.New(reg)
	d.cgroupThreadedMode = "create"
	d.cgroups = tree
	d.providers.Processes = fakeProcesses{status: map[int]string{vmInit: vmInitStatus}}
	runtime := &fakeRuntime{}
	d.containerUpdater = runtime
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{vmClaim("vm", `{"enabled": true}`)})
	require.NoError(t, err)
	require.NoError(t, results["vm"].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
	vm := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"),
		Linux: &api.LinuxContainer{CgroupsPath: "/" + root}}
	_, _, err = d.CreateContainer(ctx, pod, vm)
	require.NoError(t, err)
	require.NoError(t, d.StartContainer(ctx, pod, vm))
	share := path.Join(root, "vmm-cpu", "cpuset.cpus")
	require.Equal(t, "0,2-4", tree.read(t, share))

	results, err = d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{withConfig(claimWithResults("excl",
		claimResult{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, exclusiveParams)})
	require.NoError(t, err)
	require.NoError(t, results["excl"].Err)
	app := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: claimEnv("excl", "cpus")}
	_, _, err = d.CreateContainer(ctx, pod, app)
	require.NoError(t, err)
	require.Equal(t, "0,4", tree.read(t, share), "the take narrows the group holding the share")
	require.Equal(t, "1,5", tree.read(t, path.Join(root, "vcpu", "cpuset.cpus")), "a group of exclusive CPUs keeps them")

	_, err = d.StopContainer(ctx, pod, app)
	require.NoError(t, err)
	unprepared, err := d.UnprepareResourceClaims(ctx, []kubeletplugin.NamespacedObject{{UID: "excl"}})
	require.NoError(t, err)
	require.NoError(t, unprepared["excl"])
	d.reconcileSharedContainers(testr.New(t))
	require.Equal(t, "0,2-4", tree.read(t, share), "the give-back widens it again")

	for name := range tree.MapFS {
		if strings.HasPrefix(name, path.Join(root, "vmm-cpu")) {
			delete(tree.MapFS, name)
		}
	}
	results, err = d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{withConfig(claimWithResults("again",
		claimResult{request: "cpus", written: "2", consumed: "2", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, exclusiveParams)})
	require.NoError(t, err)
	require.NoError(t, results["again"].Err)
	_, _, err = d.CreateContainer(ctx, pod, &api.Container{Id: "app2", Name: "app2", PodSandboxId: pod.Id, Env: claimEnv("again", "cpus")})
	require.NoError(t, err, "a failed rewrite never fails the take")
	require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": "create", "result": "rewrite_error"}))
}

// Synchronize keeps the thread root of a threaded container it lists again,
// and judges it by no rule on a capped share beside another request.
func TestSynchronizeKeepsAThreadedContainer(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	const root = "kubepods/burstable/podvm/vm"
	tree := newWritableTree(t, map[string]string{root: fmt.Sprintf("%d\n", vmInit)})
	d, pod := threadedDriver(t, "create", tree, nil)
	d.ownCgroupFound = true
	events := &capturingRecorder{}
	d.eventRecorder = events
	vm := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"),
		Linux: &api.LinuxContainer{CgroupsPath: "/" + root, Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-5"}}}, State: api.ContainerState_CONTAINER_RUNNING}
	require.NoError(t, d.StartContainer(ctx, pod, vm))

	updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{vm})
	require.NoError(t, err)
	require.Empty(t, events.lines(), "no rule kept a threaded container")
	require.Len(t, updates, 1)
	require.Nil(t, updates[0].GetLinux().GetResources().GetCpu().GetQuota(), "a threaded container's cgroup gets no quota")
	require.Equal(t, root, d.podState.ThreadedContainers()["vm"].ThreadRoot)
}

// Synchronize keeps a running container that breaks a rule CreateContainer
// applies to threaded claims bound to its claims, and says so in a Warning on
// its pod and in the counter of containers kept under an older rule.
func TestSynchronizeKeepsThreadedContainersUnderOlderRules(t *testing.T) {
	for _, tc := range []struct {
		name      string
		claims    []*resourceapi.ResourceClaim
		env       []string
		wantRules map[string]float64
		wantEvent string
	}{{
		name: "a claim that is not threaded beside a threaded one",
		claims: []*resourceapi.ResourceClaim{vmClaim("vm", `{"enabled": true}`),
			claimWithResults("extra", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"})},
		env:       append(claimEnv("vm", "vcpu", "vmm-cpu"), claimEnv("extra", "io")...),
		wantRules: map[string]float64{"unthreaded_with_threaded": 1, "duplicate_group_name": 0, "duplicate_place": 0},
		wantEvent: `Warning KeptUnderOlderRule container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule unthreaded_with_threaded: claim "extra" is not threaded but shares the container with threaded claim "vm"`,
	}, {
		name: "two threaded claims giving one group name",
		claims: []*resourceapi.ResourceClaim{
			withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
			withConfig(claimWithResults("b", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
		},
		env:       append(claimEnv("a", "vcpu"), claimEnv("b", "vcpu")...),
		wantRules: map[string]float64{"unthreaded_with_threaded": 0, "duplicate_group_name": 1, "duplicate_place": 0},
		wantEvent: `Warning KeptUnderOlderRule container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule duplicate_group_name: claims "a" and "b" both have group "vcpu"; it runs without its groups and their limits`,
	}, {
		name: "two threaded claims that both set place",
		claims: []*resourceapi.ResourceClaim{
			withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "place": "vcpu"}`)),
			withConfig(claimWithResults("b", claimResult{request: "io", written: "0", consumed: "0", device: "cpudevnuma000"}),
				resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true, "place": "io"}`)),
		},
		env:       append(claimEnv("a", "vcpu"), claimEnv("b", "io")...),
		wantRules: map[string]float64{"unthreaded_with_threaded": 0, "duplicate_group_name": 0, "duplicate_place": 1},
		wantEvent: `Warning KeptUnderOlderRule container "app" was created under an older rule and keeps its claims until it is created again, when it is refused by rule duplicate_place: claims "a" and "b" both set place`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			reg := prometheus.NewRegistry()
			d := sharingDriver(t, true, nil, cpumetrics.New(reg))
			d.cgroupThreadedMode = "place"
			d.ownCgroupFound = true
			events := &capturingRecorder{}
			d.eventRecorder = events
			results, err := d.PrepareResourceClaims(ctx, tc.claims)
			require.NoError(t, err)
			for uid, result := range results {
				require.NoError(t, result.Err, "claim %s", uid)
			}
			pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
			ctr := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Env: tc.env, State: api.ContainerState_CONTAINER_RUNNING}
			_, err = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{ctr})
			require.NoError(t, err)
			require.Equal(t, []string{tc.wantEvent}, events.lines())
			for rule, want := range tc.wantRules {
				require.Equal(t, want, metricValue(t, reg, "dra_cpu_containers_kept_under_older_rule_total", map[string]string{"rule": rule}), rule)
			}
			for _, claim := range tc.claims {
				owner, bound := d.allocationState.Owner(claim.UID)
				require.Truef(t, bound, "claim %s stays bound", claim.UID)
				require.Equal(t, store.OwnerIdent{PodUID: "pod", ContainerName: "app"}, owner)
			}
		})
	}
}

// Synchronize finds each running threaded container's thread root by the
// kernel's mark and one of its groups, wherever the process sits, makes the
// groups that are missing and hands their files to the user the process runs
// as now; where it finds none it builds the groups where the init is, moving
// no process. A repair that fails is logged and counted, and fails nothing.
func TestSynchronizeRepairsThreadedTrees(t *testing.T) {
	const (
		runcCgroup = "kubepods/burstable/podvm/vm"
		crunScope  = "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podvm.slice/cri-containerd-vm.scope"
	)
	init := fmt.Sprintf("%d\n", vmInit)
	started := cgroupfs.Owner{UID: 2038693885, GID: 2038693886}
	now := cgroupfs.Owner{UID: 65532, GID: 65533}
	for _, tc := range []struct {
		name        string
		mode        string
		cgroupsPath string
		procs       map[string]string
		state       api.ContainerState
		started     bool
		damage      func(tree *writableTree)
		wantRoot    string
		wantMade    []string
		wantKept    []string
		wantPlaced  bool
		wantErrors  float64
	}{{
		name:        "a tree missing one group",
		mode:        "create",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: init},
		started:     true,
		damage: func(tree *writableTree) {
			for name := range tree.MapFS {
				if strings.HasPrefix(name, runcCgroup+"/vmm-cpu") {
					delete(tree.MapFS, name)
				}
			}
			tree.MapFS[runcCgroup+"/vcpu/cpuset.cpus"].Data = []byte("0-5")
		},
		wantRoot: runcCgroup,
		wantMade: []string{"vmm-cpu"},
		wantKept: []string{"vcpu"},
	}, {
		name:        "a thread root below the container's cgroup, the process in its place group",
		mode:        "place",
		cgroupsPath: "kubepods-burstable-podvm.slice:cri-containerd:vm",
		procs:       map[string]string{crunScope: "", crunScope + "/container": init},
		started:     true,
		damage: func(tree *writableTree) {
			tree.MapFS[crunScope+"/container/cgroup.procs"].Data = nil
			for name := range tree.MapFS {
				if strings.HasPrefix(name, crunScope+"/container/vcpu") {
					delete(tree.MapFS, name)
				}
			}
		},
		wantRoot:   crunScope + "/container",
		wantMade:   []string{"vcpu"},
		wantKept:   []string{"vmm-cpu"},
		wantPlaced: true,
	}, {
		name:        "a container whose start the driver missed",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: init},
		wantRoot:    runcCgroup,
		wantMade:    []string{"vcpu", "vmm-cpu"},
	}, {
		name:        "a missed start whose init is outside the container's cgroup",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: "", "kubepods/elsewhere": init},
		wantErrors:  1,
	}, {
		name:        "a created container, which has no cgroup yet",
		mode:        "place",
		cgroupsPath: "/" + runcCgroup,
		procs:       map[string]string{runcCgroup: ""},
		state:       api.ContainerState_CONTAINER_CREATED,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			tree := newWritableTree(t, tc.procs)
			reg := prometheus.NewRegistry()
			d, pod := threadedDriver(t, tc.mode, tree, reg)
			d.ownCgroupFound = true
			state := tc.state
			if state == api.ContainerState_CONTAINER_UNKNOWN {
				state = api.ContainerState_CONTAINER_RUNNING
			}
			vm := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"), State: state,
				Linux: &api.LinuxContainer{CgroupsPath: tc.cgroupsPath, Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-5"}}}}
			if tc.started {
				require.NoError(t, d.StartContainer(ctx, pod, vm))
				tc.damage(tree)
			}
			d.providers.Processes = fakeProcesses{
				status: map[int]string{vmInit: fmt.Sprintf("Uid:\t1\t1\t1\t%d\nGid:\t1\t1\t1\t%d\n", now.UID, now.GID)},
				cgroup: map[int]string{vmInit: "0::/kubepods/elsewhere\n"},
			}

			_, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{vm})
			require.NoError(t, err, "a repair never fails Synchronize")
			require.Equal(t, tc.wantErrors, counterOrZero(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": tc.mode, "result": "repair_error"}))
			if tc.wantRoot == "" {
				require.Empty(t, d.podState.ThreadedContainers())
				for name := range tree.MapFS {
					require.NotContains(t, name, "vcpu", "no group is built")
				}
				return
			}
			require.Equal(t, tc.wantRoot, d.podState.ThreadedContainers()["vm"].ThreadRoot)
			for _, want := range []struct{ group, cpus, max, weight string }{
				{group: "vcpu", cpus: "1,5", max: "max 100000", weight: "1"},
				{group: "vmm-cpu", cpus: "0,2-4", max: "25000 100000", weight: "10000"},
			} {
				child := path.Join(tc.wantRoot, want.group)
				require.Equal(t, "threaded", tree.read(t, path.Join(child, "cgroup.type")), want.group)
				require.Equal(t, want.cpus, tree.read(t, path.Join(child, "cpuset.cpus")), want.group)
				require.Equal(t, want.max, tree.read(t, path.Join(child, "cpu.max")), want.group)
				require.Equal(t, want.weight, tree.read(t, path.Join(child, "cpu.weight")), want.group)
			}
			for _, group := range tc.wantMade {
				child := path.Join(tc.wantRoot, group)
				require.Equal(t, now, tree.owners[path.Join(child, "cgroup.threads")], "a group made now is handed to the process's user now: %s", group)
				require.Equal(t, now, tree.owners[path.Join(child, "cgroup.procs")], group)
			}
			for _, group := range tc.wantKept {
				require.Equal(t, started, tree.owners[path.Join(tc.wantRoot, group, "cgroup.threads")], "a group that was there keeps its owner: %s", group)
			}
			placed := tree.read(t, path.Join(tc.wantRoot, "vmm-cpu", "cgroup.procs"))
			if tc.wantPlaced {
				require.Equal(t, "4242", placed)
			} else {
				require.Empty(t, placed, "Synchronize moves no process")
			}
		})
	}
}

// counterOrZero reads a counter whose series is made on its first count.
func counterOrZero(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if metricLabelsMatch(metric, labels) {
				return metricSampleValue(metric)
			}
		}
	}
	return 0
}

// A created container the kubelet never started and its running retry share
// a name: Synchronize keeps the running one's state, in either order, and
// repairs its groups alone.
func TestSynchronizeKeepsTheRunningContainerOfAName(t *testing.T) {
	const root = "kubepods/burstable/podvm/retry"
	for _, order := range []string{"orphan first", "retry first"} {
		t.Run(order, func(t *testing.T) {
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			tree := newWritableTree(t, map[string]string{root: fmt.Sprintf("%d\n", vmInit)})
			d, pod := threadedDriver(t, "create", tree, nil)
			d.ownCgroupFound = true
			orphan := &api.Container{Id: "orphan", Name: "compute", PodSandboxId: pod.Id, Env: claimEnv("vm", "vcpu", "vmm-cpu"), State: api.ContainerState_CONTAINER_CREATED,
				Linux: &api.LinuxContainer{CgroupsPath: "/kubepods/burstable/podvm/orphan"}}
			retry := &api.Container{Id: "retry", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit, Env: claimEnv("vm", "vcpu", "vmm-cpu"), State: api.ContainerState_CONTAINER_RUNNING,
				Linux: &api.LinuxContainer{CgroupsPath: "/" + root, Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0-5"}}}}
			listed := []*api.Container{orphan, retry}
			if order == "retry first" {
				listed = []*api.Container{retry, orphan}
			}
			_, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, listed)
			require.NoError(t, err)
			state := d.podState.GetContainerState("pod", "compute")
			require.NotNil(t, state)
			require.True(t, state.MatchesContainer("compute", "retry"), "the running container's state is kept")
			require.Equal(t, map[types.UID]store.ThreadedContainer{"retry": {ThreadRoot: root, ClaimRequests: []store.ClaimRequestRef{
				{ClaimUID: "vm", Request: "vcpu"}, {ClaimUID: "vm", Request: "vmm-cpu"}}}}, d.podState.ThreadedContainers())
			owner, bound := d.allocationState.Owner("vm")
			require.True(t, bound)
			require.Equal(t, store.OwnerIdent{PodUID: "pod", ContainerName: "compute"}, owner)
		})
	}
}

// Synchronize repairs nothing for a container kept under the rule against two
// claims giving one group name: one group built for both is what the rule
// keeps out.
func TestSynchronizeRepairsNoSharedGroup(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	const root = "kubepods/burstable/podvm/app"
	tree := newWritableTree(t, map[string]string{root: fmt.Sprintf("%d\n", vmInit)})
	d := sharingDriver(t, true, nil, nil)
	d.cgroupThreadedMode = "create"
	d.cgroups = tree
	d.ownCgroupFound = true
	d.eventRecorder = &capturingRecorder{}
	d.providers.Processes = fakeProcesses{status: map[int]string{vmInit: vmInitStatus}}
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{
		withConfig(claimWithResults("a", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
		withConfig(claimWithResults("b", claimResult{request: "vcpu", written: "1", consumed: "1", device: "cpudevnuma000-vms"}),
			resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`)),
	})
	require.NoError(t, err)
	for uid, result := range results {
		require.NoError(t, result.Err, "claim %s", uid)
	}
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
	ctr := &api.Container{Id: "app", Name: "app", PodSandboxId: pod.Id, Pid: vmInit, Env: append(claimEnv("a", "vcpu"), claimEnv("b", "vcpu")...),
		State: api.ContainerState_CONTAINER_RUNNING, Linux: &api.LinuxContainer{CgroupsPath: "/" + root}}
	_, err = d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err)
	_, built := tree.MapFS[path.Join(root, "vcpu")]
	require.False(t, built, "no group is built for both claims")
	require.Empty(t, d.podState.ThreadedContainers())
}

// Synchronize writes no quota for a threaded claim's capped share, which its
// group's cgroup holds, though it is the container's one request.
func TestSynchronizeGivesAThreadedShareNoQuota(t *testing.T) {
	ctx := ctxlog.NewContext(context.Background(), testr.New(t))
	d := sharingDriver(t, true, nil, nil)
	d.cgroupThreadedMode = "create"
	d.ownCgroupFound = true
	results, err := d.PrepareResourceClaims(ctx, []*resourceapi.ResourceClaim{withConfig(claimWithResults("solo",
		claimResult{request: "vmm-cpu", written: "250m", consumed: "250m", device: "cpudevnuma000"}),
		resourceapi.AllocationConfigSourceClaim, nil, threadedParams(`{"enabled": true}`))})
	require.NoError(t, err)
	require.NoError(t, results["solo"].Err)
	pod := &api.PodSandbox{Id: "sandbox", Uid: "pod", Name: "vm", Namespace: "vms"}
	ctr := &api.Container{Id: "solo", Name: "app", PodSandboxId: pod.Id, Env: claimEnv("solo", "vmm-cpu"),
		Linux: &api.LinuxContainer{Resources: &api.LinuxResources{Cpu: &api.LinuxCPU{Cpus: "0,2-4"}}}, State: api.ContainerState_CONTAINER_RUNNING}
	updates, err := d.Synchronize(ctx, []*api.PodSandbox{pod}, []*api.Container{ctr})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "0,2-4", updates[0].GetLinux().GetResources().GetCpu().GetCpus())
	require.Nil(t, updates[0].GetLinux().GetResources().GetCpu().GetQuota(), "the cap is its group's, not the container's")
}

// expiringContext is a context whose deadline passes after its first check.
type expiringContext struct {
	context.Context
	checks int
}

func (c *expiringContext) Err() error {
	c.checks++
	if c.checks > 1 {
		return context.DeadlineExceeded
	}
	return nil
}

// Under place, StartContainer moves the container's process into its claim's
// place group, last, from the cgroup namespace of PID 1 where the startup
// check found a move has to enter it, and moves nothing once the runtime has
// stopped waiting.
func TestStartContainerPlacesTheProcess(t *testing.T) {
	const root = "kubepods/burstable/podvm/vm"
	init := fmt.Sprintf("%d\n", vmInit)
	for _, tc := range []struct {
		name       string
		mover      *fakeMover
		expiring   bool
		readOnly   bool
		wantPlaced bool
		wantErr    string
	}{{
		name:       "from the driver's own cgroup namespace",
		wantPlaced: true,
	}, {
		name:       "from PID 1's cgroup namespace",
		mover:      &fakeMover{},
		wantPlaced: true,
	}, {
		name:     "after the runtime stopped waiting",
		expiring: true,
	}, {
		name:     "a move the tree refuses",
		readOnly: true,
		wantErr:  `container "compute": move its process into group "vmm-cpu": write "4242" to kubepods/burstable/podvm/vm/vmm-cpu/cgroup.procs: write kubepods/burstable/podvm/vm/vmm-cpu/cgroup.procs: read-only file system`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tree := newWritableTree(t, map[string]string{root: init})
			reg := prometheus.NewRegistry()
			d, pod := threadedDriver(t, "place", tree, reg)
			if tc.mover != nil {
				d.hostCgroups = tc.mover
			}
			var fsys cgroupfs.FS = tree
			if tc.readOnly {
				fsys = &refusingTree{writableTree: tree, refused: path.Join(root, "vmm-cpu", "cgroup.procs")}
			}
			d.cgroups = fsys
			ctx := ctxlog.NewContext(context.Background(), testr.New(t))
			if tc.expiring {
				ctx = &expiringContext{Context: ctx}
			}
			ctr := &api.Container{Id: "vm", Name: "compute", PodSandboxId: pod.Id, Pid: vmInit,
				Env: claimEnv("vm", "vcpu", "vmm-cpu"), Linux: &api.LinuxContainer{CgroupsPath: "/" + root}}
			err := d.StartContainer(ctx, pod, ctr)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Equal(t, 1.0, metricValue(t, reg, "dra_cpu_threaded_containers_total", map[string]string{"mode": "place", "result": "error"}))
				return
			}
			require.NoError(t, err)
			placed := tree.read(t, path.Join(root, "vmm-cpu", "cgroup.procs"))
			if !tc.wantPlaced {
				require.Empty(t, placed, "a process is never moved late")
				require.Equal(t, root, d.podState.ThreadedContainers()["vm"].ThreadRoot, "its groups are built")
				return
			}
			require.Equal(t, "4242", placed)
			if tc.mover != nil {
				require.Equal(t, []string{"4242 to " + path.Join(root, "vmm-cpu")}, tc.mover.moves)
			}
		})
	}
}

// refusingTree refuses writes to one file, as a read-only mount does.
type refusingTree struct {
	*writableTree
	refused string
}

func (r *refusingTree) WriteFile(name string, data []byte) error {
	if name == r.refused {
		return &fs.PathError{Op: "write", Path: name, Err: syscall.EROFS}
	}
	return r.writableTree.WriteFile(name, data)
}

// Under place the driver announces on its Node that a container may start
// unplaced; under create it moves nothing, and says nothing.
func TestAnnouncePlaceWindow(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want []string
	}{
		{mode: "place", want: []string{"Warning ThreadedPlacementBestEffort `cgroupThreadedMode: place`: a container created while the driver is slow or down starts outside its groups, on all its claim's CPUs and without their limits, until it is restarted"}},
		{mode: "create"},
		{mode: ""},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			d := sharingDriver(t, true, nil, nil)
			d.cgroupThreadedMode = tc.mode
			events := &capturingRecorder{}
			d.eventRecorder = events
			d.announcePlaceWindow(testr.New(t))
			require.Equal(t, tc.want, events.lines())
			for _, event := range events.events {
				node, ok := event.object.(*corev1.Node)
				require.True(t, ok, "the event is the Node's")
				require.Equal(t, testNodeName, node.Name)
			}
		})
	}
}
