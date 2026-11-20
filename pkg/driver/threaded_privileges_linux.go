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
	"io/fs"
	"os"
	"runtime"
	"sync"

	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	"golang.org/x/sys/unix"
)

const initCgroupNamespace = "/proc/1/ns/cgroup"

var errMoverStopped = errors.New("the driver is stopping; the process is not moved")

// hostCgroupMover moves processes from one thread that has entered the cgroup
// namespace of PID 1 and stays there for the driver's life.
type hostCgroupMover struct {
	moves    chan move
	stopped  chan struct{}
	stopOnce sync.Once
}

type move struct {
	fsys cgroupfs.FS
	dir  string
	pid  int
	done chan error
}

func (hostProcess) EnterInitCgroupNamespace() (processMover, error) {
	m := &hostCgroupMover{moves: make(chan move), stopped: make(chan struct{})}
	entered := make(chan error, 1)
	go m.run(entered)
	if err := <-entered; err != nil {
		return nil, err
	}
	return m, nil
}

func (m *hostCgroupMover) run(entered chan<- error) {
	// Never unlocked: a goroutine that exits locked takes its thread with
	// it (go1.26.0 src/runtime/proc.go:5612-5613), so no other goroutine
	// runs in the namespace entered here.
	runtime.LockOSThread()
	fd, err := unix.Open(initCgroupNamespace, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		entered <- &fs.PathError{Op: "open", Path: initCgroupNamespace, Err: err}
		return
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Setns(fd, unix.CLONE_NEWCGROUP); err != nil {
		entered <- os.NewSyscallError("setns", err)
		return
	}
	entered <- nil
	for {
		select {
		case <-m.stopped:
			return
		case mv := <-m.moves:
			mv.done <- moveFrom(fd, mv)
		}
	}
}

// moveFrom enters the namespace again and only then opens cgroup.procs: the
// kernel checks a migration against the cgroup namespace the file was opened
// in, not the writer's current one (linux v7.2
// kernel/cgroup/cgroup.c:4286-4288,5426-5434, since v5.16 commit e5745764).
func moveFrom(fd int, mv move) error {
	if err := unix.Setns(fd, unix.CLONE_NEWCGROUP); err != nil {
		return os.NewSyscallError("setns", err)
	}
	return cgroupfs.MoveProcess(mv.fsys, mv.dir, mv.pid)
}

func (m *hostCgroupMover) MoveProcess(fsys cgroupfs.FS, dir string, pid int) error {
	done := make(chan error, 1)
	select {
	case m.moves <- move{fsys: fsys, dir: dir, pid: pid, done: done}:
		return <-done
	case <-m.stopped:
		return errMoverStopped
	}
}

func (m *hostCgroupMover) Stop() {
	m.stopOnce.Do(func() { close(m.stopped) })
}
