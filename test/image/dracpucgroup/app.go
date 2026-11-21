//go:build linux

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

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"k8s.io/utils/cpuset"
)

const link = "/var/run/dracpu/cgroup"

const usage = "usage: dracpucgroup tree | dracpucgroup workload -pin <group> [-move <group>] [-threads <n>]"

type fileOwner struct {
	UID int `json:"uid"`
	GID int `json:"gid"`
}

type cgroupReport struct {
	Name           string               `json:"name"`
	Type           string               `json:"type"`
	SubtreeControl string               `json:"subtreeControl"`
	CPUs           string               `json:"cpus"`
	CPUMax         string               `json:"cpuMax"`
	Weight         string               `json:"weight"`
	Threads        []int                `json:"threads"`
	Owners         map[string]fileOwner `json:"owners"`
}

type treeReport struct {
	Link       string         `json:"link"`
	Resolved   string         `json:"resolved"`
	InitCgroup string         `json:"initCgroup"`
	Root       cgroupReport   `json:"root"`
	Groups     []cgroupReport `json:"groups"`
}

type threadReport struct {
	TID    int    `json:"tid"`
	Cgroup string `json:"cgroup"`
	CPUs   string `json:"cpus"`
}

type workloadReport struct {
	Pinned []threadReport `json:"pinned"`
	Others []threadReport `json:"others"`
	Errors []string       `json:"errors,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		fail(errors.New(usage))
	}
	switch os.Args[1] {
	case "tree":
		report, err := readTree()
		if err != nil {
			fail(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			fail(err)
		}
	case "workload":
		flags := flag.NewFlagSet("workload", flag.ExitOnError)
		pin := flags.String("pin", "", "the group the pinned threads move into")
		move := flags.String("move", "", "the group the process moves into first")
		threads := flags.Int("threads", 2, "how many threads to pin")
		_ = flags.Parse(os.Args[2:])
		if *pin == "" {
			fail(errors.New(usage))
		}
		runWorkload(*pin, *move, *threads)
	default:
		fail(errors.New(usage))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func readTree() (treeReport, error) {
	target, err := os.Readlink(link)
	if err != nil {
		return treeReport{}, err
	}
	resolved, err := filepath.EvalSymlinks(link)
	if err != nil {
		return treeReport{}, err
	}
	initCgroup, err := cgroupOf("/proc/1/cgroup")
	if err != nil {
		return treeReport{}, err
	}
	root, err := readCgroup(resolved, "")
	if err != nil {
		return treeReport{}, err
	}
	report := treeReport{Link: target, Resolved: resolved, InitCgroup: initCgroup, Root: root, Groups: []cgroupReport{}}
	entries, err := os.ReadDir(resolved)
	if err != nil {
		return treeReport{}, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		group, err := readCgroup(filepath.Join(resolved, entry.Name()), entry.Name())
		if err != nil {
			return treeReport{}, err
		}
		report.Groups = append(report.Groups, group)
	}
	return report, nil
}

func readCgroup(dir, name string) (cgroupReport, error) {
	c := cgroupReport{Name: name, Owners: map[string]fileOwner{}}
	for file, value := range map[string]*string{
		"cgroup.type":            &c.Type,
		"cgroup.subtree_control": &c.SubtreeControl,
		"cpuset.cpus":            &c.CPUs,
		"cpu.max":                &c.CPUMax,
		"cpu.weight":             &c.Weight,
	} {
		raw, err := os.ReadFile(filepath.Join(dir, file)) //nolint:gosec // a file of the cgroup tree the driver links
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return cgroupReport{}, err
		}
		*value = strings.TrimSpace(string(raw))
	}
	var err error
	if c.Threads, err = readIDs(filepath.Join(dir, "cgroup.threads")); err != nil {
		return cgroupReport{}, err
	}
	for _, file := range []string{".", "cgroup.procs", "cgroup.threads", "cgroup.subtree_control", "cpuset.cpus", "cpu.max", "cpu.weight"} {
		var st syscall.Stat_t
		if err := syscall.Stat(filepath.Join(dir, file), &st); err != nil {
			return cgroupReport{}, err
		}
		c.Owners[file] = fileOwner{UID: int(st.Uid), GID: int(st.Gid)}
	}
	return c, nil
}

func readIDs(file string) ([]int, error) {
	raw, err := os.ReadFile(file) //nolint:gosec // a file of the cgroup tree the driver links
	if err != nil {
		return nil, err
	}
	ids := []int{}
	for _, field := range strings.Fields(string(raw)) {
		id, err := strconv.Atoi(field)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// cgroupOf returns the cgroup v2 path a /proc/<pid>/cgroup file names.
func cgroupOf(file string) (string, error) {
	raw, err := os.ReadFile(file) //nolint:gosec // a file under /proc
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(raw)) {
		if path, ok := strings.CutPrefix(strings.TrimSpace(line), "0::"); ok {
			return path, nil
		}
	}
	return "", fmt.Errorf("%s has no cgroup v2 line", file)
}

// runWorkload moves the process into move, if set, starts threads threads
// that each move into pin and pin themselves to its CPUs, and prints a report
// of every thread every few seconds, for as long as the container runs.
func runWorkload(pin, move string, threads int) {
	var (
		mu     sync.Mutex
		errs   []string
		pinned []int
	)
	addError := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err.Error())
	}
	// Writing 0 moves the writer: its whole process through cgroup.procs,
	// its own thread through cgroup.threads.
	if move != "" {
		if err := os.WriteFile(filepath.Join(link, move, "cgroup.procs"), []byte("0"), 0); err != nil {
			addError(err)
		}
	}
	for range threads {
		started := make(chan int)
		go func() {
			// Never unlocked, so no other goroutine runs on a thread moved
			// out of the process's group.
			runtime.LockOSThread()
			started <- unix.Gettid()
			if err := pinThread(pin); err != nil {
				addError(err)
			}
			select {}
		}()
		pinned = append(pinned, <-started)
	}
	for {
		report, err := threadsOf(pinned)
		if err != nil {
			addError(err)
		}
		mu.Lock()
		report.Errors = slices.Clone(errs)
		mu.Unlock()
		if out, err := json.Marshal(report); err == nil {
			fmt.Println(string(out))
		}
		time.Sleep(5 * time.Second)
	}
}

// pinThread moves the calling thread into the group, then pins it to the
// group's CPUs.
func pinThread(group string) error {
	dir := filepath.Join(link, group)
	if err := os.WriteFile(filepath.Join(dir, "cgroup.threads"), []byte("0"), 0); err != nil {
		return err
	}
	raw, err := os.ReadFile(filepath.Join(dir, "cpuset.cpus")) //nolint:gosec // a file of the cgroup tree the driver links
	if err != nil {
		return err
	}
	cpus, err := cpuset.Parse(strings.TrimSpace(string(raw)))
	if err != nil {
		return err
	}
	var mask unix.CPUSet
	for _, cpu := range cpus.List() {
		mask.Set(cpu)
	}
	return unix.SchedSetaffinity(0, &mask)
}

func threadsOf(pinned []int) (workloadReport, error) {
	entries, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return workloadReport{}, err
	}
	report := workloadReport{Pinned: []threadReport{}, Others: []threadReport{}}
	for _, entry := range entries {
		tid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		dir := filepath.Join("/proc/self/task", entry.Name())
		path, err := cgroupOf(filepath.Join(dir, "cgroup"))
		if err != nil {
			return workloadReport{}, err
		}
		cpus, err := allowedCPUs(filepath.Join(dir, "status"))
		if err != nil {
			return workloadReport{}, err
		}
		t := threadReport{TID: tid, Cgroup: path, CPUs: cpus}
		if slices.Contains(pinned, tid) {
			report.Pinned = append(report.Pinned, t)
		} else {
			report.Others = append(report.Others, t)
		}
	}
	return report, nil
}

func allowedCPUs(status string) (string, error) {
	raw, err := os.ReadFile(status) //nolint:gosec // a file under /proc
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(raw)) {
		if value, ok := strings.CutPrefix(line, "Cpus_allowed_list:"); ok {
			return strings.TrimSpace(value), nil
		}
	}
	return "", fmt.Errorf("%s has no Cpus_allowed_list", status)
}
