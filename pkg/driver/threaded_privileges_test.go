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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"syscall"
	"testing"
	"testing/fstest"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr/testr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cpuinfo"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	"github.com/stretchr/testify/require"
	"k8s.io/utils/cpuset"
)

type fakeProcess struct {
	mountinfo string
	capEff    uint64
	shares    bool
	sharesErr error
	enterErr  error
	entered   bool
}

func (p *fakeProcess) MountInfo() ([]byte, error)               { return []byte(p.mountinfo), nil }
func (p *fakeProcess) EffectiveCapabilities() (uint64, error)   { return p.capEff, nil }
func (p *fakeProcess) SharesInitCgroupNamespace() (bool, error) { return p.shares, p.sharesErr }
func (p *fakeProcess) EnterInitCgroupNamespace() error {
	p.entered = true
	return p.enterErr
}

// untouchedProcess fails the test on any read.
type untouchedProcess struct{ t *testing.T }

func (p untouchedProcess) MountInfo() ([]byte, error) {
	p.t.Error("MountInfo read")
	return nil, nil
}
func (p untouchedProcess) EffectiveCapabilities() (uint64, error) {
	p.t.Error("CapEff read")
	return 0, nil
}
func (p untouchedProcess) SharesInitCgroupNamespace() (bool, error) {
	p.t.Error("cgroup namespace read")
	return false, nil
}
func (p untouchedProcess) EnterInitCgroupNamespace() error {
	p.t.Error("cgroup namespace entered")
	return nil
}

// readTree is a cgroup2 tree the driver only reads.
type readTree struct{ fstest.MapFS }

func (readTree) Mkdir(string, fs.FileMode) error { return errors.ErrUnsupported }
func (readTree) WriteFile(string, []byte) error  { return errors.ErrUnsupported }
func (readTree) Chown(string, int, int) error    { return errors.ErrUnsupported }

// Lines of /proc/self/mountinfo in a driver container: its root, the
// runtime's own read-only cgroup2 mount, and the chart's hostPath bind of the
// host's tree over it.
const (
	rootMount        = "1520 1400 0:200 / / rw,relatime - overlay overlay rw,lowerdir=/l,upperdir=/u,workdir=/w\n"
	runtimeCgroup    = "1530 1520 0:30 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup ro,nsdelegate,memory_recursiveprot\n"
	hostCgroup       = "1545 1520 0:30 / /sys/fs/cgroup rw,relatime master:9 - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n"
	hostCgroupNoDele = "1545 1520 0:30 / /sys/fs/cgroup rw,relatime master:9 - cgroup2 cgroup2 rw,memory_recursiveprot\n"
	readOnlyBind     = "1545 1520 0:30 / /sys/fs/cgroup ro,relatime master:9 - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n"
	cgroupV1         = "1545 1520 0:31 / /sys/fs/cgroup ro,nosuid,nodev,noexec master:9 - tmpfs tmpfs ro,mode=755\n"
	escapedHostRoot  = "1545 1520 0:30 / /host\\040root/sys/fs/cgroup rw,relatime master:9 - cgroup2 cgroup2 rw,nsdelegate\n"
)

// CapEff masks: containerd's default capabilities (as read on dev-fox, VF-327)
// and that set with SYS_PTRACE, SYS_ADMIN or both added.
const (
	capsDefault    = 0xa80425fb
	capsWithPtrace = 0xa80c25fb
	capsWithAdmin  = 0xa82425fb
	capsWithBoth   = 0xa82c25fb
)

func TestNewChecksThreadedPrivileges(t *testing.T) {
	openRefused := &fs.PathError{Op: "open", Path: "/proc/1/ns/cgroup", Err: syscall.EACCES}
	setnsRefused := os.NewSyscallError("setns", syscall.EPERM)
	for _, tc := range []struct {
		name          string
		mode          string
		hostRoot      string
		process       *fakeProcess
		wantErr       []string
		wantHostNS    bool
		wantTrialMade bool
	}{
		{
			name:    "create on the host's tree mounted read-write",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount + hostCgroup},
		},
		{
			name:    "create on the host's tree bound over the runtime's read-only mount",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount + runtimeCgroup + hostCgroup},
		},
		{
			name:     "create on a mount point the kernel escapes",
			mode:     "create",
			hostRoot: "/host root",
			process:  &fakeProcess{mountinfo: rootMount + escapedHostRoot},
		},
		{
			name:    "create on the runtime's read-only mount alone",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount + runtimeCgroup},
			wantErr: []string{"`cgroupThreadedMode: create` writes the host's cgroup2 tree, and the mount at /sys/fs/cgroup is read-only; mount the host's /sys/fs/cgroup there read-write, or set `cgroupThreadedMode: false`"},
		},
		{
			name:    "create on a read-only bind of a read-write cgroup2",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount + readOnlyBind},
			wantErr: []string{"`cgroupThreadedMode: create`", "the mount at /sys/fs/cgroup is read-only"},
		},
		{
			name:    "create on cgroup v1",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount + cgroupV1},
			wantErr: []string{"`cgroupThreadedMode: create` writes the host's cgroup2 tree, and the mount at /sys/fs/cgroup is tmpfs, not cgroup2; run the node on cgroup v2, or set `cgroupThreadedMode: false`"},
		},
		{
			name:    "create with nothing at the mount point",
			mode:    "create",
			process: &fakeProcess{mountinfo: rootMount},
			wantErr: []string{"`cgroupThreadedMode: create` writes the host's cgroup2 tree, and nothing is mounted at /sys/fs/cgroup; mount the host's /sys/fs/cgroup there read-write, or set `cgroupThreadedMode: false`"},
		},
		{
			name:    "place without nsdelegate",
			mode:    "place",
			process: &fakeProcess{mountinfo: rootMount + hostCgroupNoDele, capEff: capsDefault},
		},
		{
			name:    "place on cgroup v1",
			mode:    "place",
			process: &fakeProcess{mountinfo: rootMount + cgroupV1},
			wantErr: []string{"`cgroupThreadedMode: place` writes the host's cgroup2 tree, and the mount at /sys/fs/cgroup is tmpfs, not cgroup2"},
		},
		{
			name:    "place in the cgroup namespace of PID 1",
			mode:    "place",
			process: &fakeProcess{mountinfo: rootMount + hostCgroup, shares: true, capEff: capsDefault},
		},
		{
			name:          "place in a cgroup namespace of its own",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithBoth},
			wantHostNS:    true,
			wantTrialMade: true,
		},
		{
			name: "place where reading PID 1's namespace is refused",
			mode: "place",
			process: &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithBoth,
				sharesErr: &fs.PathError{Op: "stat", Path: "/proc/1/ns/cgroup", Err: syscall.EACCES}},
			wantHostNS:    true,
			wantTrialMade: true,
		},
		{
			name:          "place without CAP_SYS_PTRACE",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithAdmin, enterErr: openRefused},
			wantErr:       []string{"`cgroupThreadedMode: place` moves other containers' processes, which nsdelegate on the host's cgroup2 mount allows only from the host's cgroup namespace, and the driver cannot enter it without CAP_SYS_PTRACE (open /proc/1/ns/cgroup: permission denied); add SYS_PTRACE to the driver container's capabilities"},
			wantTrialMade: true,
		},
		{
			name:          "place without CAP_SYS_ADMIN",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithPtrace, enterErr: setnsRefused},
			wantErr:       []string{"cannot enter it without CAP_SYS_ADMIN (setns: operation not permitted); add SYS_ADMIN to the driver container's capabilities"},
			wantTrialMade: true,
		},
		{
			name:          "place with neither capability",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsDefault, enterErr: openRefused},
			wantErr:       []string{"cannot enter it without CAP_SYS_PTRACE and CAP_SYS_ADMIN", "add SYS_PTRACE and SYS_ADMIN to the driver container's capabilities"},
			wantTrialMade: true,
		},
		{
			name:          "place refused at the open with both capabilities",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithBoth, enterErr: openRefused},
			wantErr:       []string{"`cgroupThreadedMode: place` moves other containers' processes, which nsdelegate on the host's cgroup2 mount allows only from the host's cgroup namespace, and entering it was refused though the driver holds CAP_SYS_PTRACE and CAP_SYS_ADMIN (open /proc/1/ns/cgroup: permission denied), so a user namespace or a security module refused it, such as containerd's default AppArmor profile; run the driver in the host's user namespace, with an AppArmor profile that allows it, such as Unconfined"},
			wantTrialMade: true,
		},
		{
			name:          "place refused at setns with both capabilities",
			mode:          "place",
			process:       &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithBoth, enterErr: setnsRefused},
			wantErr:       []string{"refused though the driver holds CAP_SYS_PTRACE and CAP_SYS_ADMIN (setns: operation not permitted), so a user namespace or a security module refused it"},
			wantTrialMade: true,
		},
		{
			name: "place where comparing the namespaces fails otherwise",
			mode: "place",
			process: &fakeProcess{mountinfo: rootMount + hostCgroup, capEff: capsWithBoth,
				sharesErr: &fs.PathError{Op: "stat", Path: "/proc/1/ns/cgroup", Err: syscall.ENOENT}},
			wantErr: []string{"`cgroupThreadedMode: place`: compare the driver's cgroup namespace with PID 1's: stat /proc/1/ns/cgroup: no such file or directory"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOST_ROOT", tc.hostRoot)
			d, err := New(testr.New(t), Providers{
				CPUInfo:  &cpuinfo.MockCPUInfoProvider{CPUInfos: mockCPUInfos_SingleSocket_4CPUS_HT},
				SysFS:    testSysFS(mockCPUInfos_SingleSocket_4CPUS_HT),
				CgroupFS: readTree{fstest.MapFS{}},
				Process:  tc.process,
			}, &Config{
				DriverName:         testDriverName,
				NodeName:           testNodeName,
				ReservedCPUs:       cpuset.New(),
				CPUDeviceMode:      device.CPU_DEVICE_MODE_GROUPED,
				CPUDeviceGroupBy:   device.GROUP_BY_NUMA_NODE,
				CgroupThreadedMode: tc.mode,
			})
			require.Equal(t, tc.wantTrialMade, tc.process.entered, "the attempt to enter PID 1's cgroup namespace")
			if len(tc.wantErr) > 0 {
				for _, want := range tc.wantErr {
					require.ErrorContains(t, err, want)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.mode, d.cgroupThreadedMode)
			require.Equal(t, tc.wantHostNS, d.cgroupHostNamespace)
		})
	}
}

// TestNewChecksNothingWithoutThreadedCgroups: cgroupThreadedMode false, the
// empty mode here, reads nothing of the driver's process, as upstream's driver.
func TestNewChecksNothingWithoutThreadedCgroups(t *testing.T) {
	d, err := New(testr.New(t), Providers{
		CPUInfo: &cpuinfo.MockCPUInfoProvider{CPUInfos: mockCPUInfos_SingleSocket_4CPUS_HT},
		SysFS:   testSysFS(mockCPUInfos_SingleSocket_4CPUS_HT),
		Process: untouchedProcess{t},
	}, &Config{
		DriverName:       testDriverName,
		NodeName:         testNodeName,
		ReservedCPUs:     cpuset.New(),
		CPUDeviceMode:    device.CPU_DEVICE_MODE_GROUPED,
		CPUDeviceGroupBy: device.GROUP_BY_NUMA_NODE,
	})
	require.NoError(t, err)
	require.Nil(t, d.cgroups)
}

func driverPid(t *testing.T) uint32 {
	t.Helper()
	pid, err := strconv.ParseUint(strconv.Itoa(os.Getpid()), 10, 32)
	require.NoError(t, err, "the pid fits NRI's pid field")
	return uint32(pid)
}

func TestSynchronizeFindsTheDriversOwnProcess(t *testing.T) {
	pid := driverPid(t)
	procs := fmt.Sprintf("17\n%d\n", pid)
	const (
		runcCgroup  = "kubepods/besteffort/poddrv/driver"
		systemdPath = "kubepods-besteffort-poddrv.slice:cri-containerd:driver"
		crunScope   = "kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-poddrv.slice/cri-containerd-driver.scope"
	)
	driverContainer := func(pid uint32, cgroupsPath string) *api.Container {
		return &api.Container{Id: "driver", Name: "dracpu", PodSandboxId: "driver-pod", Pid: pid,
			Linux: &api.LinuxContainer{CgroupsPath: cgroupsPath}}
	}
	for _, tc := range []struct {
		name       string
		mode       string
		containers []*api.Container
		tree       fstest.MapFS
		wantErr    []string
	}{
		{
			name:       "under runc on the cgroupfs driver",
			mode:       "create",
			containers: []*api.Container{driverContainer(pid, "/"+runcCgroup)},
			tree:       fstest.MapFS{runcCgroup + "/cgroup.procs": {Data: []byte(procs)}},
		},
		{
			name:       "in crun's sub-cgroup under the systemd driver",
			mode:       "place",
			containers: []*api.Container{driverContainer(pid, systemdPath)},
			tree: fstest.MapFS{
				crunScope + "/cgroup.procs":           {Data: []byte("")},
				crunScope + "/container/cgroup.procs": {Data: []byte(procs)},
			},
		},
		{
			name:       "a runtime listing no container of the driver's pid",
			mode:       "create",
			containers: []*api.Container{driverContainer(1, "/"+runcCgroup)},
			tree:       fstest.MapFS{runcCgroup + "/cgroup.procs": {Data: []byte(procs)}},
			wantErr: []string{fmt.Sprintf("`cgroupThreadedMode: create` reads the host's processes, and the runtime lists no container whose process is the driver's, pid %d; run the driver in the host's pid namespace (`hostPID: true`), as its container's first process",
				pid)},
		},
		{
			name:       "a mount not showing the driver's cgroup",
			mode:       "place",
			containers: []*api.Container{driverContainer(pid, "/"+runcCgroup)},
			tree:       fstest.MapFS{"cgroup.procs": {Data: []byte(procs)}},
			wantErr: []string{
				"`cgroupThreadedMode: place` writes the host's cgroup2 tree, and the mount at /sys/fs/cgroup does not show the driver's own cgroup \"/kubepods/besteffort/poddrv/driver\"",
				"file does not exist; mount the host's /sys/fs/cgroup there",
			},
		},
		{
			name:       "a mount not showing the driver's process in its cgroup",
			mode:       "create",
			containers: []*api.Container{driverContainer(pid, "/"+runcCgroup)},
			tree: fstest.MapFS{
				runcCgroup + "/cgroup.procs": {Data: []byte("17\n")},
				"cgroup.procs":               {Data: []byte(procs)},
			},
			wantErr: []string{fmt.Sprintf("`cgroupThreadedMode: create` writes the host's cgroup2 tree, and the mount at /sys/fs/cgroup does not show the driver's process, pid %d, at or below its cgroup \"/kubepods/besteffort/poddrv/driver\"; mount the host's /sys/fs/cgroup there",
				pid)},
		},
		{
			name:       "false looks for nothing",
			mode:       "",
			containers: []*api.Container{driverContainer(1, "/"+runcCgroup)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := newMetricsTestDriverWithCPUInfos(t, mockCPUInfos_SingleSocket_4CPUS_HT)
			d.cgroupThreadedMode = tc.mode
			d.cgroups = readTree{tc.tree}
			d.cgroupMount = "/sys/fs/cgroup"
			var cause error
			d.cancelRun = func(err error) { cause = err }

			_, err := d.Synchronize(t.Context(), []*api.PodSandbox{{Id: "driver-pod", Uid: "poddrv"}}, tc.containers)
			if len(tc.wantErr) > 0 {
				for _, want := range tc.wantErr {
					require.ErrorContains(t, err, want)
				}
				require.ErrorIs(t, cause, ErrFatal)
				require.ErrorContains(t, cause, tc.wantErr[0])
				require.False(t, d.hasSynchronized, "Prepare stays refused")
				return
			}
			require.NoError(t, err)
			require.NoError(t, cause)
			require.True(t, d.hasSynchronized)

			_, err = d.Synchronize(t.Context(), nil, nil)
			require.NoError(t, err, "a later Synchronize looks again")
		})
	}
}
