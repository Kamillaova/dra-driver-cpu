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
	"io/fs"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

const initCgroupNamespace = "/proc/1/ns/cgroup"

func (hostProcess) EnterInitCgroupNamespace() error {
	done := make(chan error, 1)
	go func() {
		// Never unlocked: a goroutine that exits locked takes its thread with
		// it (go1.26.0 src/runtime/proc.go:5612-5613), so no other goroutine
		// runs in the namespace entered here.
		runtime.LockOSThread()
		fd, err := unix.Open(initCgroupNamespace, unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			done <- &fs.PathError{Op: "open", Path: initCgroupNamespace, Err: err}
			return
		}
		defer func() { _ = unix.Close(fd) }()
		if err := unix.Setns(fd, unix.CLONE_NEWCGROUP); err != nil {
			done <- os.NewSyscallError("setns", err)
			return
		}
		done <- nil
	}()
	return <-done
}
