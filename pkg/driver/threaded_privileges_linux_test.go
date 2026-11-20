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
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The kernel test runs where DRACPU_CGROUP_TEST names a directory to create
// under a cgroup2 mount at /sys/fs/cgroup that carries nsdelegate, as root.
const (
	cgroupTestEnv   = "DRACPU_CGROUP_TEST"
	moverHelperEnv  = "DRACPU_MOVER_HELPER"
	moverTargetEnv  = "DRACPU_MOVER_TARGET"
	moverProcessEnv = "DRACPU_MOVER_PID"
)

// TestHostCgroupMoverOnTheKernel runs the driver's mover where a container
// runs the driver: in a cgroup namespace of its own, under nsdelegate. Moving
// another cgroup's process from there is refused with ENOENT, and so is a
// write to a cgroup.procs opened before entering PID 1's namespace; the mover
// opens it after, and moves the process.
func TestHostCgroupMoverOnTheKernel(t *testing.T) {
	if os.Getenv(moverHelperEnv) != "" {
		moverHelper(t)
		return
	}
	dir := os.Getenv(cgroupTestEnv)
	if dir == "" || os.Geteuid() != 0 {
		t.Skipf("set %s to a directory name and run as root on a node whose cgroup2 mount carries nsdelegate", cgroupTestEnv)
	}
	root := path.Join(cgroupfs.Root, dir)
	for _, sub := range []string{"", "driver", "src", "dst"} {
		require.NoError(t, os.Mkdir(path.Join(root, sub), 0o755))
	}
	sleep := exec.Command("sleep", "300")
	sleep.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: openDir(t, path.Join(root, "src"))}
	require.NoError(t, sleep.Start())
	t.Cleanup(func() {
		_ = sleep.Process.Kill()
		_ = sleep.Wait()
		for _, sub := range []string{"driver", "src", "dst", ""} {
			_ = os.Remove(path.Join(root, sub))
		}
	})

	helper := exec.Command(os.Args[0], "-test.run=^TestHostCgroupMoverOnTheKernel$", "-test.v")
	helper.Env = append(os.Environ(), moverHelperEnv+"=1", moverTargetEnv+"="+dir, moverProcessEnv+"="+strconv.Itoa(sleep.Process.Pid))
	helper.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWCGROUP, UseCgroupFD: true, CgroupFD: openDir(t, path.Join(root, "driver"))}
	out, err := helper.CombinedOutput()
	t.Logf("helper:\n%s", out)
	require.NoError(t, err)

	cgroup, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", sleep.Process.Pid))
	require.NoError(t, err)
	require.Equal(t, "0::/"+path.Join(dir, "dst")+"\n", string(cgroup), "the mover moved the process")
}

func openDir(t *testing.T, dir string) int {
	t.Helper()
	fd, err := unix.Open(dir, unix.O_DIRECTORY|unix.O_RDONLY|unix.O_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })
	return fd
}

// moverHelper runs in the cgroup namespace rooted at the test's driver
// cgroup, as the driver runs in its container's.
func moverHelper(t *testing.T) {
	dir := os.Getenv(moverTargetEnv)
	pid, err := strconv.Atoi(os.Getenv(moverProcessEnv))
	require.NoError(t, err)
	self, err := os.ReadFile("/proc/self/cgroup")
	require.NoError(t, err)
	require.Equal(t, "0::/\n", string(self), "the helper is the root of a cgroup namespace of its own")
	fsys, err := cgroupfs.Host()
	require.NoError(t, err)
	dst := path.Join(dir, "dst")

	require.ErrorIs(t, cgroupfs.MoveProcess(fsys, dst, pid), syscall.ENOENT, "nsdelegate refuses a move between cgroups outside the writer's namespace")

	openedFirst := make(chan error, 1)
	go func() {
		runtime.LockOSThread()
		procs, err := os.OpenFile(path.Join(cgroupfs.Root, dst, "cgroup.procs"), os.O_WRONLY, 0)
		if err != nil {
			openedFirst <- err
			return
		}
		defer func() { _ = procs.Close() }()
		ns, err := unix.Open(initCgroupNamespace, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			openedFirst <- err
			return
		}
		defer func() { _ = unix.Close(ns) }()
		if err := unix.Setns(ns, unix.CLONE_NEWCGROUP); err != nil {
			openedFirst <- err
			return
		}
		_, err = procs.WriteString(strconv.Itoa(pid))
		openedFirst <- err
	}()
	require.ErrorIs(t, <-openedFirst, syscall.ENOENT, "a cgroup.procs opened before entering PID 1's namespace keeps the namespace it was opened in")

	mover, err := hostProcess{}.EnterInitCgroupNamespace()
	require.NoError(t, err)
	defer mover.Stop()
	require.NoError(t, mover.MoveProcess(fsys, dst, pid))
	stillHere, err := os.ReadFile("/proc/self/cgroup")
	require.NoError(t, err)
	require.Equal(t, "0::/\n", string(stillHere), "only the mover's thread entered PID 1's namespace")
	require.False(t, slices.Contains(strings.Fields(readProcs(t, dir)), strconv.Itoa(pid)), "the process left its cgroup")
}

func readProcs(t *testing.T, dir string) string {
	t.Helper()
	procs, err := os.ReadFile(path.Join(cgroupfs.Root, dir, "src", "cgroup.procs"))
	require.NoError(t, err)
	return string(procs)
}
