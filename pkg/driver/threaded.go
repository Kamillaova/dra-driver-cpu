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
	"maps"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/cgroupfs"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/cpuset"
)

// ContainerProcesses reads, through the host's pid namespace, what the
// threaded build needs of another container's process.
type ContainerProcesses interface {
	// Status returns /proc/<pid>/status.
	Status(pid int) ([]byte, error)
	// Cgroup returns /proc/<pid>/cgroup.
	Cgroup(pid int) ([]byte, error)
}

type hostProcesses struct{}

func (hostProcesses) Status(pid int) ([]byte, error) {
	return os.ReadFile(path.Join("/proc", strconv.Itoa(pid), "status"))
}

func (hostProcesses) Cgroup(pid int) ([]byte, error) {
	return os.ReadFile(path.Join("/proc", strconv.Itoa(pid), "cgroup"))
}

const reasonPlacementBestEffort = "ThreadedPlacementBestEffort"

const placeWindowMessage = "`cgroupThreadedMode: place`: a container created while the driver is slow or down starts outside its groups, on all its claim's CPUs and without their limits, until it is restarted"

// announcePlaceWindow says, under place, on the driver's log and its Node,
// that a container may start unplaced: NRI lets a container start when its
// call to the driver times out or no driver is connected, unless the runtime
// requires the driver's plugin, and a process is never moved once it runs.
func (cp *CPUDriver) announcePlaceWindow(logger logr.Logger) {
	if cp.cgroupThreadedMode != cgroupfs.ThreadedModePlace {
		return
	}
	logger.Info(placeWindowMessage)
	cp.eventRecorder.Event(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cp.nodeName}}, corev1.EventTypeWarning, reasonPlacementBestEffort, placeWindowMessage)
}

// threadedModeAttribute is the mode as every device publishes it.
func threadedModeAttribute(mode string) string {
	if !threadedModeOn(mode) {
		return "off"
	}
	return mode
}

func claimName(record store.ClaimRecord, uid types.UID) string {
	if record.Name == "" {
		return string(uid)
	}
	return record.Name
}

// checkThreadedContainer reports whether a container holds a threaded claim,
// refusing one that also holds a claim of this driver that is not threaded,
// whose requests would sit in the thread root, one whose threaded claims give
// two groups one name, counting the groups their allocations build, and one
// whose threaded claims both set place, its process starting in one group.
func checkThreadedContainer(container string, allocations *store.AllocationState, claims map[types.UID]draEnvClaim) (bool, error) {
	c := containerClaimsOf(allocations, slices.Sorted(maps.Keys(claims)))
	if len(c.threaded) == 0 {
		return false, nil
	}
	if err, found := c.unthreadedBeside(container); found {
		return true, err
	}
	if err, found := c.twoPlaces(container); found {
		return true, err
	}
	if err, found := c.sharedGroup(container); found {
		return true, err
	}
	return true, nil
}

// threadedRulesBroken returns the rules on a threaded container that a running
// container's bound claims break, each with the refusal CreateContainer gives,
// without its advice.
func threadedRulesBroken(container string, allocations *store.AllocationState, claimUIDs []types.UID) []olderRule {
	c := containerClaimsOf(allocations, claimUIDs)
	if len(c.threaded) == 0 {
		return nil
	}
	var broken []olderRule
	if err, found := c.unthreadedBeside(container); found {
		broken = append(broken, olderRule{rule: cpumetrics.RuleUnthreadedWithThreaded, detail: err.detail()})
	}
	if err, found := c.twoPlaces(container); found {
		broken = append(broken, olderRule{rule: cpumetrics.RuleDuplicatePlace, detail: err.detail()})
	}
	if err, found := c.sharedGroup(container); found {
		broken = append(broken, olderRule{rule: cpumetrics.RuleDuplicateGroupName, detail: err.detail()})
	}
	return broken
}

// containerClaims are the claims of a container this driver records, threaded
// and not, in order of UID.
type containerClaims struct {
	threaded, unthreaded []types.UID
	records              map[types.UID]store.ClaimRecord
}

func containerClaimsOf(allocations *store.AllocationState, uids []types.UID) containerClaims {
	c := containerClaims{records: make(map[types.UID]store.ClaimRecord, len(uids))}
	for _, uid := range uids {
		record, ok := allocations.GetClaimRecord(uid)
		if !ok {
			continue
		}
		c.records[uid] = record
		if record.Layout != nil {
			c.threaded = append(c.threaded, uid)
		} else {
			c.unthreaded = append(c.unthreaded, uid)
		}
	}
	return c
}

func (c containerClaims) name(uid types.UID) string {
	return claimName(c.records[uid], uid)
}

func (c containerClaims) unthreadedBeside(container string) (unthreadedClaimError, bool) {
	if len(c.threaded) == 0 || len(c.unthreaded) == 0 {
		return unthreadedClaimError{}, false
	}
	return unthreadedClaimError{container: container, claim: c.name(c.unthreaded[0]), threaded: c.name(c.threaded[0])}, true
}

func (c containerClaims) twoPlaces(container string) (twoPlacesError, bool) {
	var placing []types.UID
	for _, uid := range c.threaded {
		if c.records[uid].Layout.Place != "" {
			placing = append(placing, uid)
		}
	}
	if len(placing) < 2 {
		return twoPlacesError{}, false
	}
	return twoPlacesError{container: container, first: c.name(placing[0]), second: c.name(placing[1])}, true
}

func (c containerClaims) sharedGroup(container string) (sharedGroupError, bool) {
	builtBy := map[string]types.UID{}
	for _, uid := range c.threaded {
		for _, group := range recordGroups(c.records[uid]) {
			if other, taken := builtBy[group]; taken {
				return sharedGroupError{container: container, first: c.name(other), second: c.name(uid), group: group}, true
			}
			builtBy[group] = uid
		}
	}
	return sharedGroupError{}, false
}

type unthreadedClaimError struct {
	container, claim, threaded string
}

func (err unthreadedClaimError) Error() string {
	return fmt.Sprintf("container %q: claim %q is not threaded but shares the container with threaded claim %q; move its requests into the threaded claim or another container",
		err.container, err.claim, err.threaded)
}

func (err unthreadedClaimError) detail() string {
	return fmt.Sprintf("claim %q is not threaded but shares the container with threaded claim %q", err.claim, err.threaded)
}

type twoPlacesError struct {
	container, first, second string
}

func (err twoPlacesError) Error() string {
	return fmt.Sprintf("container %q: claims %q and %q both set place; keep place in one claim",
		err.container, err.first, err.second)
}

func (err twoPlacesError) detail() string {
	return fmt.Sprintf("claims %q and %q both set place", err.first, err.second)
}

type sharedGroupError struct {
	container, first, second, group string
}

func (err sharedGroupError) Error() string {
	return fmt.Sprintf("container %q: claims %q and %q both have group %q; give the groups different names",
		err.container, err.first, err.second, err.group)
}

func (err sharedGroupError) detail() string {
	return fmt.Sprintf("claims %q and %q both have group %q; it runs without its groups and their limits", err.first, err.second, err.group)
}

// recordGroups returns the groups a threaded claim's allocation builds, in
// order of name.
func recordGroups(record store.ClaimRecord) []string {
	var groups []string
	for _, request := range record.Requests {
		if !slices.Contains(groups, request.Group) {
			groups = append(groups, request.Group)
		}
	}
	slices.Sort(groups)
	return groups
}

// threadedGroupsOf returns the groups the threaded claims among uids build,
// in order of name: each with the CPUs its requests run on now and the cap of
// the one capped request it may hold, weighed together under their thread
// root.
func (cp *CPUDriver) threadedGroupsOf(uids []types.UID) ([]cgroupfs.Group, error) {
	type built struct {
		group  cgroupfs.Group
		shared []int64
	}
	byName := map[string]*built{}
	for _, uid := range uids {
		record, ok := cp.allocationState.GetClaimRecord(uid)
		if !ok || record.Layout == nil {
			continue
		}
		for _, request := range record.Requests {
			b := byName[request.Group]
			if b == nil {
				b = &built{group: cgroupfs.Group{Name: request.Group, CPUs: cpuset.New(), Period: cfsPeriodUS}}
				byName[request.Group] = b
			}
			cpus, err := cp.allocationState.GetRequestAllocationUnion(store.ClaimRequestRef{ClaimUID: uid, Request: request.Request})
			if err != nil {
				return nil, err
			}
			b.group.CPUs = b.group.CPUs.Union(cpus)
			if request.Role != store.RoleShared {
				continue
			}
			b.shared = append(b.shared, request.Amount)
			if request.Cap > 0 {
				b.group.Quota = cfsQuota(request.Cap)
			}
		}
	}
	names := slices.Sorted(maps.Keys(byName))
	shared := make([][]int64, 0, len(names))
	for _, name := range names {
		shared = append(shared, byName[name].shared)
	}
	weights := cgroupfs.Weights(shared)
	groups := make([]cgroupfs.Group, 0, len(names))
	for i, name := range names {
		g := byName[name].group
		g.Weight = weights[i]
		groups = append(groups, g)
	}
	return groups, nil
}

// buildThreadedTree builds, at a threaded container's start, its groups under
// its thread root: the cgroup of the container's cgroup subtree, that cgroup
// included, whose cgroup.procs lists the container's init. No name below the
// container's cgroup is assumed: crun under the systemd driver and its
// run.oci.delegate-cgroup annotation hold the init in a sub-cgroup of their
// own. It hands the files that move processes and threads into the groups to
// the user the init runs as, as the host sees it. Under place it then moves
// the process into its claim's place group, unless the runtime has stopped
// waiting: a process moved late would take its new group's cpuset over every
// thread the workload pinned meanwhile (linux v7.2 kernel/sched/core.c:3207-3210).
func (cp *CPUDriver) buildThreadedTree(ctx context.Context, logger logr.Logger, pod *api.PodSandbox, ctr *api.Container, claims map[types.UID]draEnvClaim) error {
	uids := slices.Sorted(maps.Keys(claims))
	if !slices.ContainsFunc(uids, func(uid types.UID) bool {
		record, ok := cp.allocationState.GetClaimRecord(uid)
		return ok && record.Layout != nil
	}) {
		return nil
	}
	if !threadedModeOn(cp.cgroupThreadedMode) {
		logger.Info("building no threaded groups: the node runs cgroupThreadedMode false")
		return nil
	}
	// A container created while the driver was away was never refused, and one
	// group built for two claims is what the refusal keeps out.
	if shared, found := containerClaimsOf(cp.allocationState, uids).sharedGroup(ctr.GetName()); found {
		logger.Info("building no threaded groups: the container was created without the driver, which refuses it", "reason", shared.Error())
		return nil
	}
	root, err := cp.buildTree(ctr, uids)
	if err == nil {
		err = cp.place(ctx, logger, ctr, uids, root)
	}
	if err != nil {
		cp.metrics.RecordThreadedContainer(cp.cgroupThreadedMode, cpumetrics.ResultError)
		return err
	}
	cp.podState.SetThreadRoot(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()), root)
	cp.metrics.RecordThreadedContainer(cp.cgroupThreadedMode, cpumetrics.ResultSuccess)
	logger.V(2).Info("built the container's threaded groups", "threadRoot", root)
	return nil
}

func (cp *CPUDriver) buildTree(ctr *api.Container, uids []types.UID) (string, error) {
	dir, err := cgroupfs.Dir(ctr.GetLinux().GetCgroupsPath())
	if err != nil {
		return "", fmt.Errorf("container %q: %w", ctr.GetName(), err)
	}
	pid := int(ctr.GetPid())
	root, err := cgroupfs.FindProcess(cp.cgroups, dir, pid)
	if err != nil {
		return "", fmt.Errorf("container %q: %w", ctr.GetName(), err)
	}
	if root == "" {
		return "", cp.initOutside(ctr.GetName(), pid, dir)
	}
	groups, err := cp.threadedGroupsOf(uids)
	if err != nil {
		return "", fmt.Errorf("container %q: %w", ctr.GetName(), err)
	}
	for _, g := range groups {
		if g.CPUs.IsEmpty() {
			return "", fmt.Errorf("container %q: group %q has no CPUs left", ctr.GetName(), g.Name)
		}
	}
	owner, err := cp.processOwner(pid)
	if err != nil {
		return "", fmt.Errorf("container %q: %w", ctr.GetName(), err)
	}
	if err := cgroupfs.Build(cp.cgroups, root, groups, owner); err != nil {
		return "", fmt.Errorf("container %q: %w", ctr.GetName(), err)
	}
	return root, nil
}

// place moves the container's process into the place group of its threaded
// claims, under place, from the cgroup namespace of PID 1 where the startup
// check found that a move has to enter it. Writing a threaded group's
// cgroup.procs moves every thread of the process (cgroup-v2.rst:396-397).
func (cp *CPUDriver) place(ctx context.Context, logger logr.Logger, ctr *api.Container, uids []types.UID, root string) error {
	if cp.cgroupThreadedMode != cgroupfs.ThreadedModePlace {
		return nil
	}
	var group string
	for _, uid := range uids {
		if record, ok := cp.allocationState.GetClaimRecord(uid); ok && record.Layout != nil && record.Layout.Place != "" {
			group = record.Layout.Place
		}
	}
	if group == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		logger.Info("not moving the container's process: the runtime stopped waiting for the driver, so it starts in its thread root", "group", group, "err", err.Error())
		return nil
	}
	dir := path.Join(root, group)
	pid := int(ctr.GetPid())
	move := cgroupfs.MoveProcess
	if cp.hostCgroups != nil {
		move = cp.hostCgroups.MoveProcess
	}
	if err := move(cp.cgroups, dir, pid); err != nil {
		return fmt.Errorf("container %q: move its process into group %q: %w", ctr.GetName(), group, err)
	}
	logger.V(2).Info("moved the container's process into its place group", "group", group)
	return nil
}

// initOutside refuses a container whose init sits outside its cgroup's
// subtree, naming the cgroup the init is in.
func (cp *CPUDriver) initOutside(container string, pid int, dir string) error {
	where := "an unknown cgroup"
	if raw, err := cp.providers.EnsureProcesses().Cgroup(pid); err == nil {
		for line := range strings.Lines(string(raw)) {
			if cgroup, ok := strings.CutPrefix(strings.TrimSpace(line), "0::"); ok {
				where = strconv.Quote(cgroup)
			}
		}
	}
	return fmt.Errorf("container %q: init runs in %s, outside the container's cgroup %q, so threaded groups cannot be built", container, where, "/"+dir)
}

// processOwner returns the filesystem user and group a process runs as, as
// the reader's user namespace, the host's, sees them: the fourth field of the
// Uid and Gid lines of its status (linux v7.2 fs/proc/array.c:149-152).
func (cp *CPUDriver) processOwner(pid int) (cgroupfs.Owner, error) {
	status, err := cp.providers.EnsureProcesses().Status(pid)
	if err != nil {
		return cgroupfs.Owner{}, fmt.Errorf("read the status of pid %d: %w", pid, err)
	}
	ids := map[string]int{}
	for line := range strings.Lines(string(status)) {
		key, value, ok := strings.Cut(line, ":")
		if !ok || (key != "Uid" && key != "Gid") {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) != 4 {
			return cgroupfs.Owner{}, fmt.Errorf("parse the %s line of pid %d: %q", key, pid, strings.TrimSpace(line))
		}
		id, err := strconv.Atoi(fields[3])
		if err != nil {
			return cgroupfs.Owner{}, fmt.Errorf("parse the %s line of pid %d: %w", key, pid, err)
		}
		ids[key] = id
	}
	uid, hasUID := ids["Uid"]
	gid, hasGID := ids["Gid"]
	if !hasUID || !hasGID {
		return cgroupfs.Owner{}, fmt.Errorf("the status of pid %d has no Uid or Gid line", pid)
	}
	return cgroupfs.Owner{UID: uid, GID: gid}, nil
}

// rewriteThreadedShares writes, in every threaded container whose thread root
// the driver found, the CPUs each group holding a share runs on now, which
// change as exclusive requests take and give back CPUs of its partition. A
// failure is logged and counted and fails nothing: the container may be
// gone. Callers hold stateMu.
func (cp *CPUDriver) rewriteThreadedShares(logger logr.Logger) {
	if cp.cgroups == nil {
		return
	}
	threaded := cp.podState.ThreadedContainers()
	for _, id := range slices.Sorted(maps.Keys(threaded)) {
		c := threaded[id]
		var uids []types.UID
		for _, ref := range c.ClaimRequests {
			if !slices.Contains(uids, ref.ClaimUID) {
				uids = append(uids, ref.ClaimUID)
			}
		}
		groups, err := cp.threadedGroupsOf(uids)
		if err != nil {
			logger.V(2).Info("not rewriting the groups of a container whose claims are no longer prepared", "containerID", id, "err", err.Error())
			continue
		}
		for _, g := range groups {
			if !cp.groupHoldsShare(uids, g.Name) {
				continue
			}
			if g.CPUs.IsEmpty() {
				logger.Error(errNoSharedRequestCPUs, "leaving a threaded group on its CPUs", "containerID", id, "group", g.Name)
				continue
			}
			if err := cgroupfs.SetCPUs(cp.cgroups, path.Join(c.ThreadRoot, g.Name), g.CPUs); err != nil {
				logger.Error(err, "the threaded group keeps its CPUs until it is rewritten again", "containerID", id, "group", g.Name)
				cp.metrics.RecordThreadedContainer(cp.cgroupThreadedMode, cpumetrics.ResultRewriteError)
			}
		}
	}
}

// groupHoldsShare reports whether group holds a shared request of a threaded claim
// among uids.
func (cp *CPUDriver) groupHoldsShare(uids []types.UID, group string) bool {
	for _, uid := range uids {
		record, ok := cp.allocationState.GetClaimRecord(uid)
		if ok && slices.ContainsFunc(record.Requests, func(r store.RequestAllocation) bool {
			return r.Group == group && r.Role == store.RoleShared
		}) {
			return true
		}
	}
	return false
}
