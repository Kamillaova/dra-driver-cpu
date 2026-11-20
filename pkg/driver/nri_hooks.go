/*
Copyright 2025 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/nri/pkg/api"
	"github.com/go-logr/logr"
	dracpuapi "github.com/kubernetes-sigs/dra-driver-cpu/api"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/ctxlog"
	cpumetrics "github.com/kubernetes-sigs/dra-driver-cpu/pkg/metrics"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/cpuset"
	cdiparser "tags.cncf.io/container-device-interface/pkg/parser"
)

// Configure is called by NRI on each connection, before Synchronize.
func (cp *CPUDriver) Configure(_ context.Context, _, runtime, version string) (api.EventMask, error) {
	defer cp.watchdog.track("Configure")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	cp.runtimeReportsCDIDevices = reportsCDIDevices(runtime, version)
	// No events: the stub then subscribes to every event the driver handles.
	return 0, nil
}

// containerd's version is a link-time string: v2.4.0 from its Makefile's git
// describe, with -3-g<sha> or .m after a patched or dirty tree, or
// 2.4.0+unknown where the build sets none.
var (
	containerdVersion  = regexp.MustCompile(`^v?(\d+)\.(\d+)(?:\.(\d+))?(.*)$`)
	gitDescribeCommits = regexp.MustCompile(`^-\d+-g[0-9a-f]+`)
)

// reportsCDIDevices reports whether the runtime fills in a container's CDI
// devices for NRI, which containerd does from 2.3.0 on. A version that does not
// parse reads as one that does not.
func reportsCDIDevices(runtime, version string) bool {
	if runtime != "containerd" {
		return false
	}
	parts := containerdVersion.FindStringSubmatch(version)
	if parts == nil {
		return false
	}
	major, err := strconv.Atoi(parts[1])
	if err != nil {
		return false
	}
	minor, err := strconv.Atoi(parts[2])
	if err != nil {
		return false
	}
	if major != 2 {
		return major > 2
	}
	if minor != 3 {
		return minor > 3
	}
	preRelease := (parts[3] == "" || parts[3] == "0") && strings.HasPrefix(parts[4], "-") && !gitDescribeCommits.MatchString(parts[4])
	return !preRelease
}

var errDeviceNotInjected = errors.New("the runtime did not inject its CDI device into the container")

// checkInjected refuses a DRA_CPUSET_* variable whose claim or request the
// runtime did not inject the CDI device of. A pod spec can set the variable,
// but only the kubelet asks the runtime for a device, for the requests the pod
// references. Call it only where the runtime reports CDI devices: there an
// empty list means none.
func checkInjected(allocations *store.AllocationState, ctr *api.Container, claims map[types.UID]draEnvClaim) error {
	injected := sets.New[string]()
	for _, device := range ctr.GetCDIDevices() {
		injected.Insert(device.GetName())
	}
	for _, uid := range slices.Sorted(maps.Keys(claims)) {
		claim := claims[uid]
		name := string(uid)
		if record, ok := allocations.GetClaimRecord(uid); ok && record.Name != "" {
			name = record.Name
		}
		if claim.upstreamForm() {
			device := cdiparser.QualifiedName(cdiVendor, cdiClass, getCDIDeviceName(uid))
			if !injected.Has(device) {
				return fmt.Errorf("%w: container %q names claim %q in %s_%s, and %s is not among its devices; reference the claim in the pod's resourceClaims and the container's resources.claims, and set no %s_* variable",
					errDeviceNotInjected, ctr.GetName(), name, cdiEnvVarPrefix, uid, device, cdiEnvVarPrefix)
			}
			continue
		}
		for _, request := range claim.requests {
			device := cdiparser.QualifiedName(cdiVendor, cdiClass, getCDIRequestDeviceName(uid, request))
			if !injected.Has(device) {
				return fmt.Errorf("%w: container %q names request %q of claim %q in %s_%s_%s, and %s is not among its devices; reference the claim in the pod's resourceClaims and the request in the container's resources.claims, and set no %s_* variable",
					errDeviceNotInjected, ctr.GetName(), request, name, cdiEnvVarPrefix, uid, request, device, cdiEnvVarPrefix)
			}
		}
	}
	return nil
}

// Synchronize is called by the NRI to synchronize the state of the driver during bootstrap.
func (cp *CPUDriver) Synchronize(ctx context.Context, pods []*api.PodSandbox, containers []*api.Container) (rupdates []*api.ContainerUpdate, rerr error) {
	defer cp.watchdog.track("Synchronize")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	startTime := time.Now()
	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen))
	// Recovery runs at startup and on reconnect; always log it.
	logger.Info("begin: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))
	defer logger.Info("end: synchronize state with the runtime", "numPods", len(pods), "numContainers", len(containers))

	defer func() { cp.metrics.RecordNRISynchronize(rerr, time.Since(startTime)) }()

	// Returning the error keeps Prepare refused, the first Synchronize never
	// having ended, while the driver stops.
	if err := cp.checkOwnCgroupLocked(logger, containers); err != nil {
		cp.cancelRun(fmt.Errorf("%w: %w", ErrFatal, err))
		return nil, err
	}
	if cp.window != nil {
		cp.window.keep(pods, containers)
		cp.nriDisconnected.Store(false)
		logger.Info("keeping the runtime's containers for when the node's profile is read", "label", cp.profileLabel)
		return nil, nil
	}
	containerUpdates, err := cp.judgeContainersLocked(ctx, logger, pods, containers)
	if err != nil {
		return nil, err
	}
	cp.nriDisconnected.Store(false)
	if cp.resynced != nil {
		close(cp.resynced)
		cp.resynced = nil
	}
	logger.V(6).Info("synchronization complete", "updatesCount", len(containerUpdates))
	return containerUpdates, nil
}

// judgeContainersLocked binds the listed containers to the claims they name, by
// CreateContainer's rules, and returns the updates that pin them. It works on
// copies of the stores and swaps them in only when ctx is still live. Callers
// hold stateMu.
func (cp *CPUDriver) judgeContainersLocked(ctx context.Context, logger logr.Logger, pods []*api.PodSandbox, containers []*api.Container) ([]*api.ContainerUpdate, error) {
	// Runtime inventory is not the prepared-claim inventory: a claim may
	// have no container yet, or retain its owner between container restarts.
	// Only Unprepare releases these records while this process survives.
	allocationState := cp.allocationState.Clone()
	podState := store.NewPodState()
	var containerUpdates []*api.ContainerUpdate

	var listed []*syncContainer
	for _, pod := range pods {
		pLogger := logger.WithValues("pod", ctxlog.KObj(pod), "podUID", pod.Uid)
		pLogger.V(2).Info("synchronize pod")
		for _, container := range containers {
			if container.PodSandboxId != pod.Id {
				continue
			}
			cLogger := pLogger.WithValues("container", container.Name)

			claimAllocations, err := parseDRAEnvToClaimAllocations(cLogger, container.Env)
			if err != nil {
				// Keep tracking a known runtime container on reconnect, especially
				// a shared container that must receive future pool updates. Never
				// infer allocations from malformed env or reuse a replaced ID.
				if cp.hasSynchronized {
					podUID := types.UID(pod.GetUid())
					state := cp.podState.GetContainerState(podUID, container.Name)
					if state != nil && state.MatchesContainer(container.Name, types.UID(container.GetId())) {
						podState.SetContainerState(podUID, state)
						cLogger.Error(err, "retaining known container state with malformed DRA env during synchronize")
						continue
					}
				}
				cLogger.Error(err, "ignoring container with malformed DRA env during synchronize")
				continue
			}
			listed = append(listed, &syncContainer{
				pod:       pod,
				container: container,
				owner:     store.OwnerIdent{PodUID: types.UID(pod.GetUid()), ContainerName: container.GetName()},
				claims:    claimAllocations,
				logger:    cLogger,
			})
		}
	}

	for _, sc := range listed {
		sc.recordClaims(allocationState)
	}

	// Containers created while the driver was not connected were never judged by
	// CreateContainer. Judge them by its rules, claim by claim: NRI gives no way
	// to stop a container from here, and failing would leave every other
	// container on the node without the driver.
	contenders := contendersForUnboundClaims(allocationState, listed)
	refused := 0
	var noSharedCPUs []string
	var kept []cpumetrics.Rule
	var warnings []olderRuleWarning
	for _, sc := range listed {
		if uid, hold, held := cp.heldClaimOf(sc.claims); held {
			if hasTask(sc.container) {
				sc.logger.Info("stopping container: a reserved partition holds its claim's CPUs", "claimUID", uid, "reason", hold.refusal(uid))
				containerUpdates = append(containerUpdates, killUpdate(sc.container.GetId()))
			}
			continue
		}
		allGuaranteedCPUs := cpuset.New()
		var claimUIDs []types.UID
		var boundRefs []store.ClaimRequestRef
		keptUnder := sets.New[cpumetrics.Rule]()
		var broken []olderRule
		for _, uid := range slices.Sorted(maps.Keys(sc.claims)) {
			caLogger := sc.logger.WithValues("claimUID", uid)
			if c, ok := contenders[uid]; ok && c.excludes(sc.owner) {
				caLogger.Error(errContestedClaim, "not binding claim to container during synchronize", "reason", refusedContested, "containers", c.String())
				refused++
				continue
			}
			refs, expected, older, err := syncBinding(allocationState, uid, sc)
			var cpus cpuset.CPUSet
			if err == nil {
				cpus, err = allocationState.GetRequestAllocationUnion(refs...)
			}
			if err == nil {
				_, err = allocationState.Bind(caLogger, sc.owner.PodUID, sc.owner.ContainerName, refs, expected)
			}
			if err != nil {
				caLogger.Error(err, "not binding claim to container during synchronize", "reason", refusalReason(err))
				refused++
				continue
			}
			if older.rule != "" {
				caLogger.Info("keeping the container bound to the whole claim under an older rule: the runtime shows it pinned to its claims' CPUs", "rule", older.rule)
				keptUnder.Insert(older.rule)
				broken = append(broken, older)
			}
			allGuaranteedCPUs = allGuaranteedCPUs.Union(cpus)
			claimUIDs = append(claimUIDs, uid)
			boundRefs = append(boundRefs, refs...)
		}
		members := boundMembers(sc, allocationState, claimUIDs)
		threaded := hasLayout(allocationState, claimUIDs)
		var capped cappedBesideError
		if err := checkCappedAlone(sc.container.GetName(), members); !threaded && errors.As(err, &capped) {
			sc.logger.Info("keeping the container bound to its claims under an older rule, without a limit: NRI gives no way to stop it from here", "rule", cpumetrics.RuleLimitWithOthers)
			keptUnder.Insert(cpumetrics.RuleLimitWithOthers)
			broken = append(broken, olderRule{rule: cpumetrics.RuleLimitWithOthers, detail: capped.detail()})
		}
		for _, older := range threadedRulesBroken(sc.container.GetName(), allocationState, claimUIDs) {
			sc.logger.Info("keeping the container bound to its claims under an older rule rather than stopping it", "rule", older.rule)
			keptUnder.Insert(older.rule)
			broken = append(broken, older)
		}
		kept = append(kept, sets.List(keptUnder)...)
		if len(broken) > 0 {
			warnings = append(warnings, olderRuleWarning{pod: sc.pod, container: sc.container.GetName(), broken: broken})
		}

		state := store.NewContainerState(sc.container.GetName(), types.UID(sc.container.GetId()), claimUIDs...)
		state.SetClaimRequests(boundRefs)
		if known := cp.podState.GetContainerState(sc.owner.PodUID, sc.container.GetName()); threaded && known != nil &&
			known.MatchesContainer(sc.container.GetName(), types.UID(sc.container.GetId())) {
			state.SetThreadRoot(known.ThreadRoot())
		}
		if len(claimUIDs) > 0 && allGuaranteedCPUs.IsEmpty() {
			noSharedCPUs = append(noSharedCPUs, sc.container.GetId())
		} else if len(claimUIDs) > 0 {
			sc.logger.V(2).Info("found guaranteed CPUs", "cpus", allGuaranteedCPUs.String())

			// Reconcile guaranteed container CPU mask.
			guaranteedUpdate := &api.ContainerUpdate{
				ContainerId: sc.container.GetId(),
			}
			guaranteedUpdate.SetLinuxCPUSetCPUs(allGuaranteedCPUs.String())
			if len(broken) == 0 && !threaded {
				convergeCap(sc, members, guaranteedUpdate)
			}
			containerUpdates = append(containerUpdates, guaranteedUpdate)
		}
		podState.SetContainerState(sc.owner.PodUID, state)
		sc.logger.V(6).Info("set container state", "claims", len(claimUIDs))
	}

	if len(noSharedCPUs) > 0 {
		// NRI can neither give a container an empty cpuset nor stop it from here.
		slices.Sort(noSharedCPUs)
		logger.Error(errNoSharedRequestCPUs, "leaving containers holding only shared requests on their current CPUs", "containers", noSharedCPUs)
	}

	// Reconcile container CPU masks to handle cases where the NRI plugin might have crashed
	// or restarted and missed updating the cgroup settings.
	// See: https://github.com/containerd/nri/issues/282
	sharedContainerUpdates, err := sharedContainerUpdatesFor(logger, allocationState, podState, types.UID(""))
	switch {
	case errors.Is(err, errNoDefaultPartitionCPUs):
		containers := podState.GetContainersWithSharedCPUs()
		slices.Sort(containers)
		logger.Error(err, "leaving containers without claims on their current CPUs", "containers", containers)
	case err != nil:
		// NRI can neither give a container an empty cpuset nor stop it from here.
		logger.Error(err, "leaving containers without claims on their current CPUs until a claim is unprepared", "containers", podState.GetContainersWithSharedCPUs())
	}
	containerUpdates = append(containerUpdates, sharedContainerUpdates...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cp.podState = podState
	cp.allocationState = allocationState
	if !cp.hasSynchronized && cp.initialSync != nil {
		close(cp.initialSync)
	}
	cp.hasSynchronized = true
	cp.refreshAllocationMetrics()
	cp.rewriteThreadedShares(logger)
	for range refused {
		cp.metrics.RecordNRISynchronizeRefusedClaim()
	}
	for _, rule := range kept {
		cp.metrics.RecordContainerKeptUnderOlderRule(rule)
	}
	for _, warning := range warnings {
		cp.eventRecorder.Event(warning.object(), corev1.EventTypeWarning, reasonKeptUnderOlderRule, warning.message())
	}
	return containerUpdates, nil
}

// boundMembers returns the requests of the claims bound to a listed
// container.
func boundMembers(sc *syncContainer, allocations *store.AllocationState, claimUIDs []types.UID) []cgroupMember {
	bound := make(map[types.UID]draEnvClaim, len(claimUIDs))
	for _, uid := range claimUIDs {
		bound[uid] = sc.claims[uid]
	}
	return cgroupMembers(allocations, bound)
}

// hasLayout reports whether one of the claims is threaded.
func hasLayout(allocations *store.AllocationState, claimUIDs []types.UID) bool {
	return slices.ContainsFunc(claimUIDs, func(uid types.UID) bool {
		record, ok := allocations.GetClaimRecord(uid)
		return ok && record.Layout != nil
	})
}

// convergeCap gives a listed container the cap CreateContainer would have
// written. A CPU quota a container shows was set by the kubelet or written
// before.
func convergeCap(sc *syncContainer, members []cgroupMember, update *api.ContainerUpdate) {
	if capMilli := soleCap(members); capMilli > 0 && !hasKubeletQuota(sc.container) {
		update.SetLinuxCPUQuota(cfsQuota(capMilli))
		update.SetLinuxCPUPeriod(cfsPeriodUS)
	}
}

// draEnvClaim is what a container's DRA_CPUSET_* variables say of one claim:
// the requests they name, or, from upstream's variable, which names none, the
// claim's cpuset.
type draEnvClaim struct {
	requests []string
	cpus     cpuset.CPUSet
}

func (c draEnvClaim) upstreamForm() bool {
	return len(c.requests) == 0
}

func (c draEnvClaim) refs(uid types.UID) []store.ClaimRequestRef {
	if c.upstreamForm() {
		return []store.ClaimRequestRef{{ClaimUID: uid}}
	}
	refs := make([]store.ClaimRequestRef, 0, len(c.requests))
	for _, request := range c.requests {
		refs = append(refs, store.ClaimRequestRef{ClaimUID: uid, Request: request})
	}
	return refs
}

func parseDRAEnvToClaimAllocations(logger logr.Logger, envs []string) (map[types.UID]draEnvClaim, error) {
	allocations := make(map[types.UID]draEnvClaim)
	for _, env := range envs {
		key, value, hasValue := strings.Cut(env, "=")
		// We won't have a conflict by constructions with the vars we inject from DRA side
		// and consume from NRI side, but still we add a defensive check.
		// So this is the explicit skip for well-known public environment variable.
		if key == dracpuapi.EnvVarExclusiveAssignedCPUSet {
			continue
		}
		if !strings.HasPrefix(key, cdiEnvVarPrefix) {
			continue
		}
		logger.V(4).Info("parsing DRA env entry", "env", env)
		if !hasValue {
			return nil, fmt.Errorf("malformed DRA env entry %q", env)
		}
		name, ok := strings.CutPrefix(key, cdiEnvVarPrefix+"_")
		if !ok {
			continue
		}
		uid, request, perRequest := strings.Cut(name, "_")
		claimUID := types.UID(uid)
		claim, named := allocations[claimUID]
		if named && claim.upstreamForm() == perRequest {
			return nil, fmt.Errorf("malformed DRA env entry %q: claim %q is named both whole and by request", env, claimUID)
		}
		if perRequest {
			if request == "" {
				return nil, fmt.Errorf("malformed DRA env entry %q: no request after the claim UID", env)
			}
			if value != cdiEnvDynamicValue {
				return nil, fmt.Errorf("malformed DRA env entry %q: a variable naming a request holds %q", env, cdiEnvDynamicValue)
			}
			claim.requests = append(claim.requests, request)
		} else {
			parsedSet, err := cpuset.Parse(value)
			if err != nil {
				return nil, fmt.Errorf("failed to parse cpuset value %q from env %q: %w", value, env, err)
			}
			claim.cpus = parsedSet
		}
		allocations[claimUID] = claim
	}
	for uid, claim := range allocations {
		slices.Sort(claim.requests)
		claim.requests = slices.Compact(claim.requests)
		allocations[uid] = claim
	}

	return allocations, nil
}

func cdiSpecCPUs(logger logr.Logger, uid types.UID, envs []string) (cpuset.CPUSet, error) {
	allocations, err := parseDRAEnvToClaimAllocations(logger, envs)
	if err != nil {
		return cpuset.CPUSet{}, fmt.Errorf("failed to parse CDI env for claim %q: %w", uid, err)
	}
	claim, ok := allocations[uid]
	if !ok || !claim.upstreamForm() {
		return cpuset.CPUSet{}, fmt.Errorf("driver-owned CDI spec %q does not contain a DRA allocation for claim %q", getCDIDeviceName(uid), uid)
	}
	return claim.cpus, nil
}

type syncContainer struct {
	pod       *api.PodSandbox
	container *api.Container
	owner     store.OwnerIdent
	claims    map[types.UID]draEnvClaim
	logger    logr.Logger
	// onRecord are the claims the container names as the store records them.
	onRecord []types.UID
	// pinnedToRecord is whether the runtime shows the container pinned to
	// exactly the CPUs of the claims in onRecord, whole.
	pinnedToRecord bool
}

// recordClaims finds the claims the container names as the store records them:
// in upstream's form with the recorded cpuset, or by requests the record holds.
// containerd reports a container's cpuset from its stored spec, which holds the
// cpuset the driver set when it admitted the container, so a container pinned
// to exactly the CPUs of its claims is one a driver admitted.
func (sc *syncContainer) recordClaims(allocations *store.AllocationState) {
	recorded := cpuset.New()
	for _, uid := range slices.Sorted(maps.Keys(sc.claims)) {
		claim := sc.claims[uid]
		record, ok := allocations.GetClaimRecord(uid)
		if !ok || (claim.upstreamForm() && !record.CPUs().Equals(claim.cpus)) {
			continue
		}
		if _, err := allocations.GetRequestAllocationUnion(claim.refs(uid)...); err != nil {
			continue
		}
		whole, err := allocations.GetRequestAllocationUnion(store.ClaimRequestRef{ClaimUID: uid})
		if err != nil {
			continue
		}
		recorded = recorded.Union(whole)
		sc.onRecord = append(sc.onRecord, uid)
	}
	pinned, err := cpuset.Parse(sc.container.GetLinux().GetResources().GetCpu().GetCpus())
	sc.pinnedToRecord = err == nil && !recorded.IsEmpty() && pinned.Equals(recorded)
}

// syncBinding returns the requests of a claim to bind to a running container
// and the CPUs to compare, or why the claim is not bound to it. A container
// that names some but not all of the claim's requests, which CreateContainer
// refuses since this rule came, keeps the whole claim where the runtime shows
// it pinned to exactly the CPUs of its claims: the rule then applies at its next
// CreateContainer.
func syncBinding(allocations *store.AllocationState, uid types.UID, sc *syncContainer) ([]store.ClaimRequestRef, map[store.ClaimRequestRef]cpuset.CPUSet, olderRule, error) {
	claim := sc.claims[uid]
	refs := claim.refs(uid)
	if claim.upstreamForm() {
		return refs, map[store.ClaimRequestRef]cpuset.CPUSet{refs[0]: claim.cpus}, olderRule{}, nil
	}
	if _, err := allocations.GetRequestAllocationUnion(refs...); err != nil {
		return nil, nil, olderRule{}, err
	}
	if err := wholeClaimUse(allocations, uid, claim, sc.container.GetName()); err != nil {
		var partial partialHolderError
		if !sc.pinnedToRecord || !errors.As(err, &partial) {
			return nil, nil, olderRule{}, err
		}
		return []store.ClaimRequestRef{{ClaimUID: uid}}, nil, olderRule{rule: cpumetrics.RulePartialHolder, detail: "it " + partial.detail()}, nil
	}
	return refs, nil, olderRule{}, nil
}

// olderRule is a rule on a claim's shape that came after a running container
// was created, and that the container breaks.
type olderRule struct {
	rule   cpumetrics.Rule
	detail string
}

const reasonKeptUnderOlderRule = "KeptUnderOlderRule"

// olderRuleWarning is the Warning on the pod of a container that Synchronize
// keeps bound to its claims although it breaks rules that came after it.
type olderRuleWarning struct {
	pod       *api.PodSandbox
	container string
	broken    []olderRule
}

func (w olderRuleWarning) object() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: w.pod.GetNamespace(), Name: w.pod.GetName(), UID: types.UID(w.pod.GetUid())}}
}

func (w olderRuleWarning) message() string {
	broken := make([]string, 0, len(w.broken))
	for _, older := range w.broken {
		broken = append(broken, fmt.Sprintf("rule %s: %s", older.rule, older.detail))
	}
	return fmt.Sprintf("container %q was created under an older rule and keeps its claims until it is created again, when it is refused by %s",
		w.container, strings.Join(broken, ", and by "))
}

// wholeClaimUse refuses a container that names some but not all of a claim's
// requests, where upstream's driver gives such a container the whole claim.
func wholeClaimUse(allocations *store.AllocationState, uid types.UID, claim draEnvClaim, container string) error {
	record, ok := allocations.GetClaimRecord(uid)
	if !ok || claim.upstreamForm() {
		return nil
	}
	named := sets.New[string]()
	for _, request := range record.Requests {
		named.Insert(request.Request)
	}
	if len(claim.requests) >= named.Len() {
		return nil
	}
	name := record.Name
	if name == "" {
		name = string(uid)
	}
	return partialHolderError{container: container, claim: name, requests: claim.requests}
}

type partialHolderError struct {
	container string
	claim     string
	requests  []string
}

func (err partialHolderError) Error() string {
	return fmt.Sprintf("container %q: %s", err.container, err.detail())
}

func (err partialHolderError) detail() string {
	noun := "request"
	if len(err.requests) > 1 {
		noun = "requests"
	}
	quoted := make([]string, 0, len(err.requests))
	for _, request := range err.requests {
		quoted = append(quoted, strconv.Quote(request))
	}
	return fmt.Sprintf("uses only %s %s of claim %q, which must be used whole; use the whole claim, or give the container its own claim",
		noun, strings.Join(quoted, ", "), err.claim)
}

type syncRefusal string

const (
	refusedUnprepared     syncRefusal = "unprepared"
	refusedUnknownRequest syncRefusal = "unknown_request"
	refusedCPUMismatch    syncRefusal = "cpu_mismatch"
	refusedBoundElsewhere syncRefusal = "bound_elsewhere"
	refusedOverlap        syncRefusal = "overlap"
	refusedContested      syncRefusal = "contested"
	refusedPartialHolder  syncRefusal = "partial_holder"
)

var errContestedClaim = errors.New("several containers name the claim and the runtime does not show this one alone pinned to its CPUs: name a claim in one container only")

func refusalReason(err error) syncRefusal {
	var partial partialHolderError
	switch {
	case errors.Is(err, store.ErrUnknownRequest):
		return refusedUnknownRequest
	case errors.Is(err, store.ErrClaimCPUMismatch):
		return refusedCPUMismatch
	case errors.Is(err, store.ErrClaimAlreadyBound):
		return refusedBoundElsewhere
	case errors.Is(err, store.ErrClaimOverlap):
		return refusedOverlap
	case errors.As(err, &partial):
		return refusedPartialHolder
	}
	return refusedUnprepared
}

type contenders struct {
	owners  []store.OwnerIdent
	holders []store.OwnerIdent
}

func (c *contenders) excludes(owner store.OwnerIdent) bool {
	if len(c.owners) < 2 {
		return false
	}
	return len(c.holders) != 1 || !c.holders[0].Equal(owner)
}

func (c *contenders) String() string {
	names := make([]string, 0, len(c.owners))
	for _, owner := range c.owners {
		names = append(names, fmt.Sprintf("%s/%s", owner.PodUID, owner.ContainerName))
	}
	slices.Sort(names)
	return strings.Join(names, ",")
}

// contendersForUnboundClaims lists, for each claim nothing is bound to, the
// owners whose containers name it as the store records it, and the holders
// among them: those pinned to exactly the CPUs of their claims.
func contendersForUnboundClaims(allocations *store.AllocationState, listed []*syncContainer) map[types.UID]*contenders {
	result := make(map[types.UID]*contenders)
	for _, sc := range listed {
		for _, uid := range sc.onRecord {
			if _, bound := allocations.Owner(uid); bound {
				continue
			}
			c := result[uid]
			if c == nil {
				c = &contenders{}
				result[uid] = c
			}
			if !slices.ContainsFunc(c.owners, sc.owner.Equal) {
				c.owners = append(c.owners, sc.owner)
			}
			if sc.pinnedToRecord && !slices.ContainsFunc(c.holders, sc.owner.Equal) {
				c.holders = append(c.holders, sc.owner)
			}
		}
	}
	return result
}

// cgroupMember is one request a container holds in its cgroup.
type cgroupMember struct {
	claim    string
	request  string
	capMilli int64
}

func (m cgroupMember) String() string {
	if m.request == "" {
		return fmt.Sprintf("claim %q", m.claim)
	}
	return fmt.Sprintf("request %q of claim %q", m.request, m.claim)
}

// cgroupMembers returns the requests a container's variables name, from their
// claims' records, in order of claim UID and request.
func cgroupMembers(allocations *store.AllocationState, claims map[types.UID]draEnvClaim) []cgroupMember {
	var members []cgroupMember
	for _, uid := range slices.Sorted(maps.Keys(claims)) {
		claim := claims[uid]
		record, ok := allocations.GetClaimRecord(uid)
		if !ok {
			continue
		}
		name := record.Name
		if name == "" {
			name = string(uid)
		}
		for _, request := range record.Requests {
			if !claim.upstreamForm() && !slices.Contains(claim.requests, request.Request) {
				continue
			}
			members = append(members, cgroupMember{claim: name, request: request.Request, capMilli: request.Cap})
		}
	}
	return members
}

// checkCappedAlone refuses a container whose cgroup holds a capped share
// beside any other request: a cgroup has one cpu.max, under which an uncapped
// share would run, and which would throttle exclusive CPUs.
func checkCappedAlone(container string, members []cgroupMember) error {
	if len(members) < 2 {
		return nil
	}
	capped := slices.IndexFunc(members, func(m cgroupMember) bool { return m.capMilli > 0 })
	if capped < 0 {
		return nil
	}
	for i, other := range members {
		if i != capped && other.capMilli > 0 {
			return cappedBesideError{container: container, capped: members[capped], other: other}
		}
	}
	other := members[0]
	if capped == 0 {
		other = members[1]
	}
	return cappedBesideError{container: container, capped: members[capped], other: other}
}

type cappedBesideError struct {
	container string
	capped    cgroupMember
	other     cgroupMember
}

func (err cappedBesideError) Error() string {
	if err.other.capMilli > 0 {
		return fmt.Sprintf("container %q: %s and %s both have limits, so they cannot be used together: use the claims in separate containers, use cgroupConfig.threaded in both claims, or remove one of the limits",
			err.container, err.capped, err.other)
	}
	return fmt.Sprintf("container %q: %s has limit of %s, so it cannot be used together with %s: use the claims in separate containers, use cgroupConfig.threaded in both claims, or remove the limit",
		err.container, err.capped, resource.NewMilliQuantity(err.capped.capMilli, resource.DecimalSI), err.other)
}

func (err cappedBesideError) detail() string {
	if err.other.capMilli > 0 {
		return fmt.Sprintf("%s and %s both have limits, so they cannot be used together", err.capped, err.other)
	}
	return fmt.Sprintf("%s has limit of %s, so it cannot be used together with %s",
		err.capped, resource.NewMilliQuantity(err.capped.capMilli, resource.DecimalSI), err.other)
}

// soleCap returns the cap of a container holding one capped request alone, 0
// for any other container.
func soleCap(members []cgroupMember) int64 {
	if len(members) != 1 {
		return 0
	}
	return members[0].capMilli
}

const (
	cfsPeriodUS = 100000
	// The kernel refuses a quota below 1 ms (linux v7.2 kernel/sched/core.c:10107-10109).
	minCFSQuotaUS = 1000
)

func cfsQuota(capMilli int64) int64 {
	return max(capMilli*cfsPeriodUS/1000, minCFSQuotaUS)
}

// hasKubeletQuota reports whether the container arrives with a CPU quota,
// which containerd sets only from a non-zero quota the kubelet sends for a CPU
// limit on the container or its pod (containerd v2.4.0
// internal/cri/opts/spec_linux_opts.go:389-391). That quota stays the cap.
func hasKubeletQuota(ctr *api.Container) bool {
	return ctr.GetLinux().GetResources().GetCpu().GetQuota().GetValue() > 0
}

const reasonCPULimitReplacesCap = "CPULimitReplacesCap"

func podObject(pod *api.PodSandbox) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: pod.GetNamespace(), Name: pod.GetName(), UID: types.UID(pod.GetUid())}}
}

// capReplacedMessage is the Warning on the pod of a container whose CPU limit
// replaces the cap its claim asks for.
func capReplacedMessage(ctr *api.Container, capped cgroupMember) string {
	cpu := ctr.GetLinux().GetResources().GetCpu()
	period := int64(cfsPeriodUS)
	if p := cpu.GetPeriod().GetValue(); p > 0 && p <= math.MaxInt64 {
		period = int64(p)
	}
	limit := resource.NewMilliQuantity(cpu.GetQuota().GetValue()*1000/period, resource.DecimalSI)
	return fmt.Sprintf("container %q: its CPU limit of %s replaces the limit of %s of %s",
		ctr.GetName(), limit, resource.NewMilliQuantity(capped.capMilli, resource.DecimalSI), capped)
}

var errNoDefaultPartitionCPUs = errors.New("cannot update containers without claims: the default partition holds no CPU")

var errNoSharedRequestCPUs = errors.New("every CPU of the devices its shared requests use is held by an exclusive claim")

// noDefaultCPUsError refuses a container without claims on a node whose
// default partition holds no CPU, where it could run on none.
func (cp *CPUDriver) noDefaultCPUsError(container string) error {
	node := fmt.Sprintf("node %q", cp.nodeName)
	if cp.profileName != "" {
		node += fmt.Sprintf(" (profile %q)", cp.profileName)
	}
	return fmt.Errorf("container %q: no CPU claim, and %s leaves no CPUs for containers without one; add a claim on an exclusive partition, or run it elsewhere",
		container, node)
}

// allocationOf labels a container for the NRI metrics by the requests it
// names: shared where they hold no CPU alone, and exclusive where they do or
// are not prepared.
func allocationOf(allocations *store.AllocationState, refs []store.ClaimRequestRef) cpumetrics.CPUAllocation {
	if len(refs) == 0 {
		return cpumetrics.CPUAllocationShared
	}
	if exclusive, err := allocations.GetExclusiveRequestUnion(refs...); err == nil && exclusive.IsEmpty() {
		return cpumetrics.CPUAllocationShared
	}
	return cpumetrics.CPUAllocationExclusive
}

func wholeClaimRefs(claimUIDs []types.UID) []store.ClaimRequestRef {
	refs := make([]store.ClaimRequestRef, 0, len(claimUIDs))
	for _, uid := range claimUIDs {
		refs = append(refs, store.ClaimRequestRef{ClaimUID: uid})
	}
	return refs
}

// sharedContainerUpdatesFor computes updates from the supplied stores. Callers
// using the live driver stores must hold stateMu; Synchronize passes candidate stores.
func sharedContainerUpdatesFor(logger logr.Logger, allocations *store.AllocationState, pods *store.PodState, excludeID types.UID) ([]*api.ContainerUpdate, error) {
	updates := []*api.ContainerUpdate{}
	sharedCPUs := allocations.GetClaimlessCPUs()
	preparedCPUs := allocations.GetPreparedCPUs()
	sharedCPUContainers := pods.GetContainersWithSharedCPUs()
	if confined, ok := allocations.ConfinedClaimlessCPUs(); ok && confined.IsEmpty() && len(sharedCPUContainers) > 0 {
		return nil, errNoDefaultPartitionCPUs
	}
	// An empty CPUSet is serialized by NRI as Cpus="", which means "do not
	// change the current CPUSet" rather than "clear the CPUSet". Never emit
	// that update while a prepared DRA allocation has exhausted the pool and
	// shared containers still exist. An empty pool with no prepared allocation
	// is valid when the node has no driver-managed CPUs.
	if sharedCPUs.IsEmpty() && !preparedCPUs.IsEmpty() && len(sharedCPUContainers) > 0 {
		return nil, fmt.Errorf("cannot update shared containers: no shared CPUs available")
	}
	logger.V(2).Info("updating CPU allocation for containers without guaranteed CPUs", "sharedCPUs", sharedCPUs.String())
	for _, containerUID := range sharedCPUContainers {
		if containerUID == excludeID {
			// Skip the container being created as it is already covered in the container adjustment.
			continue
		}

		containerUpdate := &api.ContainerUpdate{
			ContainerId: string(containerUID),
		}
		containerUpdate.SetLinuxCPUSetCPUs(sharedCPUs.String())
		updates = append(updates, containerUpdate)
	}
	return updates, nil
}

// shareHolderUpdatesFor gives every container holding a shared request the
// CPUs its requests hold now, exclusive ones included: a share runs on its
// devices' CPUs less those exclusive requests take out of them, which change
// as such requests are prepared and unprepared. A holder whose shares are left
// no CPU is sent its exclusive CPUs alone; one left no CPU at all is refused,
// since NRI sends an empty cpuset as no change, which would leave it on the
// CPUs an exclusive request now holds. Callers using the live driver stores
// must hold stateMu.
func shareHolderUpdatesFor(logger logr.Logger, allocations *store.AllocationState, pods *store.PodState, excludeID types.UID) ([]*api.ContainerUpdate, error) {
	byContainer := pods.ClaimRequestsByContainer()
	var updates []*api.ContainerUpdate
	for _, containerUID := range slices.Sorted(maps.Keys(byContainer)) {
		if containerUID == excludeID {
			continue
		}
		refs := byContainer[containerUID]
		cpus, err := allocations.GetRequestAllocationUnion(refs...)
		if err != nil {
			logger.V(2).Info("not updating a container whose claims are no longer prepared", "containerID", containerUID, "err", err.Error())
			continue
		}
		if !holdsShare(allocations, refs) {
			continue
		}
		if cpus.IsEmpty() {
			return nil, fmt.Errorf("cannot update shared containers: no shared CPUs available")
		}
		update := &api.ContainerUpdate{ContainerId: string(containerUID)}
		update.SetLinuxCPUSetCPUs(cpus.String())
		updates = append(updates, update)
	}
	return updates, nil
}

// holdsShare reports whether refs name a shared request, or a whole claim
// that holds one.
func holdsShare(allocations *store.AllocationState, refs []store.ClaimRequestRef) bool {
	for _, ref := range refs {
		record, ok := allocations.GetClaimRecord(ref.ClaimUID)
		if ok && slices.ContainsFunc(record.Requests, func(held store.RequestAllocation) bool {
			return held.Role == store.RoleShared && (ref.Request == "" || held.Request == ref.Request)
		}) {
			return true
		}
	}
	return false
}

// CreateContainer handles container creation requests from the NRI.
func (cp *CPUDriver) CreateContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) (radjust *api.ContainerAdjustment, rupdates []*api.ContainerUpdate, rerr error) {
	defer cp.watchdog.track("CreateContainer")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: CreateContainer")
	defer logger.V(2).Info("end: CreateContainer")

	adjust := &api.ContainerAdjustment{}
	var updates []*api.ContainerUpdate

	alloc := cpumetrics.CPUAllocationUnknown
	claimAllocations, err := parseDRAEnvToClaimAllocations(logger, ctr.Env)
	defer func() { cp.metrics.RecordNRICreateContainer(rerr, alloc, time.Since(startTime)) }()
	if err != nil {
		logger.Error(err, "error parsing DRA env for container")
		return nil, nil, fmt.Errorf("failed to parse DRA environment for container %q in pod %q: %w", ctr.Name, pod.Uid, err)
	}
	claimCount := len(claimAllocations)
	alloc = cpumetrics.CPUAllocationShared
	if claimCount > 0 {
		alloc = cpumetrics.CPUAllocationExclusive
	}
	if cp.window != nil {
		return cp.window.admit(cp.profileLabel, pod, ctr, claimCount)
	}

	containerId := types.UID(ctr.GetId())
	podUID := types.UID(pod.GetUid())

	if claimCount == 0 {
		if defaultCPUs, confined := cp.allocationState.ConfinedClaimlessCPUs(); confined && defaultCPUs.IsEmpty() {
			return nil, nil, cp.noDefaultCPUsError(ctr.GetName())
		}
		// This is a shared container.
		sharedCPUs := cp.allocationState.GetClaimlessCPUs()
		if sharedCPUs.IsEmpty() && !cp.allocationState.GetPreparedCPUs().IsEmpty() {
			// NRI cannot represent an empty CPUSet as a ContainerAdjustment. Fail
			// closed instead of allowing the runtime to keep its default affinity.
			return nil, nil, fmt.Errorf("cannot create shared container %q in pod %q: no shared CPUs available", ctr.Name, pod.Uid)
		}
		state := store.NewContainerState(ctr.GetName(), containerId)
		cp.podState.SetContainerState(podUID, state)

		logger.V(2).Info("no guaranteed CPUs found, using shared CPUs", "sharedCPUs", sharedCPUs.String())
		adjust.SetLinuxCPUSetCPUs(sharedCPUs.String())
	} else {
		if uid, hold, held := cp.heldClaimOf(claimAllocations); held {
			return nil, nil, fmt.Errorf("cannot create container %q in pod %q: %s", ctr.Name, pod.Uid, hold.refusal(uid))
		}
		if cp.runtimeReportsCDIDevices {
			if err := checkInjected(cp.allocationState, ctr, claimAllocations); err != nil {
				logger.Error(err, "refusing container whose DRA env names a device the runtime did not inject")
				return nil, nil, fmt.Errorf("cannot bind claim to container %q in pod %q: %w", ctr.Name, pod.Uid, err)
			}
		}
		// NRI invokes CreateContainer for all containers. Only trust DRA env
		// entries that match a claim prepared by this driver.
		claimUIDs := slices.Sorted(maps.Keys(claimAllocations))
		var refs []store.ClaimRequestRef
		expected := make(map[store.ClaimRequestRef]cpuset.CPUSet)
		for _, uid := range claimUIDs {
			claim := claimAllocations[uid]
			if claim.upstreamForm() {
				expected[store.ClaimRequestRef{ClaimUID: uid}] = claim.cpus
			}
			refs = append(refs, claim.refs(uid)...)
		}
		alloc = allocationOf(cp.allocationState, refs)
		guaranteedCPUs, err := cp.allocationState.GetRequestAllocationUnion(refs...)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot bind claim to container %q in pod %q: %w", ctr.Name, pod.Uid, err)
		}
		if guaranteedCPUs.IsEmpty() {
			// NRI cannot represent an empty CPUSet as a ContainerAdjustment.
			return nil, nil, fmt.Errorf("cannot create container %q in pod %q: %w", ctr.Name, pod.Uid, errNoSharedRequestCPUs)
		}
		exclusiveCPUs, err := cp.allocationState.GetExclusiveRequestUnion(refs...)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot bind claim to container %q in pod %q: %w", ctr.Name, pod.Uid, err)
		}
		for _, uid := range claimUIDs {
			if err := wholeClaimUse(cp.allocationState, uid, claimAllocations[uid], ctr.GetName()); err != nil {
				return nil, nil, err
			}
		}
		members := cgroupMembers(cp.allocationState, claimAllocations)
		threaded, err := checkThreadedContainer(ctr.GetName(), cp.allocationState, claimAllocations)
		if err != nil {
			return nil, nil, err
		}
		// A threaded container's caps go to its groups' cgroups, each holding
		// one capped request alone.
		if !threaded {
			if err := checkCappedAlone(ctr.GetName(), members); err != nil {
				return nil, nil, err
			}
		}
		updatedClaims, err := cp.allocationState.Bind(logger, podUID, ctr.Name, refs, expected)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot bind claim to container %q in pod %q: %w", ctr.Name, pod.Uid, err)
		}
		logger.V(2).Info("guaranteed CPUs found", "cpus", guaranteedCPUs.String())
		state := store.NewContainerState(ctr.GetName(), containerId, claimUIDs...)
		state.SetClaimRequests(refs)
		adjust.SetLinuxCPUSetCPUs(guaranteedCPUs.String())
		if !exclusiveCPUs.IsEmpty() {
			adjust.AddEnv(dracpuapi.EnvVarExclusiveAssignedCPUSet, exclusiveCPUs.String())
		}
		if capped := slices.IndexFunc(members, func(m cgroupMember) bool { return m.capMilli > 0 }); threaded && capped >= 0 && hasKubeletQuota(ctr) {
			// The kubelet's quota on the container's cgroup also bounds every
			// group below it.
			cp.eventRecorder.Event(podObject(pod), corev1.EventTypeWarning, reasonCPULimitReplacesCap, capReplacedMessage(ctr, members[capped]))
		} else if capMilli := soleCap(members); capMilli > 0 && !threaded {
			if hasKubeletQuota(ctr) {
				cp.eventRecorder.Event(podObject(pod), corev1.EventTypeWarning, reasonCPULimitReplacesCap, capReplacedMessage(ctr, members[0]))
			} else {
				adjust.SetLinuxCPUQuota(cfsQuota(capMilli))
				adjust.SetLinuxCPUPeriod(cfsPeriodUS)
			}
		}
		// An updated claim means this is the first CreateContainer after Prepare, so
		// existing shared containers must be moved off the newly claimed CPUs.
		// On restart the owner already exists and no shared-container updates are
		// needed.
		if len(updatedClaims) > 0 {
			updates, err = sharedContainerUpdatesFor(logger, cp.allocationState, cp.podState, containerId)
			if err == nil {
				var holderUpdates []*api.ContainerUpdate
				holderUpdates, err = shareHolderUpdatesFor(logger, cp.allocationState, cp.podState, containerId)
				updates = append(updates, holderUpdates...)
			}
			if err != nil {
				cp.allocationState.Unbind(logger, updatedClaims...)
				return nil, nil, fmt.Errorf("cannot update shared CPU pool after creating container %q in pod %q: %w", ctr.Name, pod.Uid, err)
			}
			cp.rewriteThreadedShares(logger)
		}
		cp.podState.SetContainerState(podUID, state)
	}

	return adjust, updates, nil
}

// StartContainer refuses to start a container of a claim a reserved partition
// holds. Such a container was created before the driver started, and an
// update killing it while it has no task only rewrites its stored spec
// (containerd v2.4.0 internal/cri/server/container_update_resources.go:120-124).
// It then builds a threaded container's groups, after runc create and before
// the task starts (containerd v2.4.0 internal/cri/server/container_start.go:172-176),
// and never waits or calls the runtime.
func (cp *CPUDriver) StartContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	defer cp.watchdog.track("StartContainer")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if cp.window != nil {
		return nil
	}
	_, logger := ctxlog.WithValues(ctx, "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	claims, err := parseDRAEnvToClaimAllocations(logger, ctr.Env)
	if err != nil || len(claims) == 0 {
		return nil
	}
	if uid, hold, held := cp.heldClaimOf(claims); held {
		return fmt.Errorf("cannot start container %q in pod %q: %s", ctr.Name, pod.Uid, hold.refusal(uid))
	}
	return cp.buildThreadedTree(ctx, logger, pod, ctr, claims)
}

// StopContainer removes runtime container state without changing DRA-owned allocations.
//
// CPU-allocation lifetime across the DRA and NRI hooks:
//   - PrepareResourceClaims (DRA) reserves CPUs and writes the CDI spec carrying that cpuset.
//   - CreateContainer (NRI) validates the CDI cpuset and applies it to the container.
//   - StopContainer (NRI, here) removes only the matching runtime container state. The prepared
//     allocation and owner remain unchanged so a restarted container reuses the same CPUs.
//   - UnprepareResourceClaims (DRA) is the authoritative release point for the allocation and owner.
//   - Synchronize rebuilds runtime container state, retaining prepared claims and owners
//     while this process survives. Cold recovery uses running containers and CDI.
func (cp *CPUDriver) StopContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) ([]*api.ContainerUpdate, error) {
	defer cp.watchdog.track("StopContainer")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: StopContainer")
	defer logger.V(2).Info("end: StopContainer")

	updates := []*api.ContainerUpdate{}
	if cp.window != nil {
		cp.window.drop(ctr.GetId())
		return updates, nil
	}
	claimUIDs, removed := cp.podState.RemoveContainerState(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()))
	if !removed {
		logger.V(2).Info("ignoring stale or unknown StopContainer event")
		return updates, nil
	}

	// leverage the fact the hook can't fail in our current design
	cp.metrics.RecordNRIStopContainer(nil, allocationOf(cp.allocationState, wholeClaimRefs(claimUIDs)), time.Since(startTime))
	return updates, nil
}

// RemoveContainer handles container removal requests from the NRI.
func (cp *CPUDriver) RemoveContainer(ctx context.Context, pod *api.PodSandbox, ctr *api.Container) error {
	defer cp.watchdog.track("RemoveContainer")()
	cp.stateMu.Lock()
	defer cp.stateMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	startTime := time.Now()

	_, logger := ctxlog.WithValues(ctx, "opID", generateShortID(opIDLen), "pod", ctxlog.KObj(pod), "podUID", pod.Uid, "container", ctr.Name, "containerID", ctr.Id)
	logger.V(2).Info("begin: RemoveContainer")
	defer logger.V(2).Info("end: RemoveContainer")

	if cp.window != nil {
		cp.window.drop(ctr.GetId())
		return nil
	}
	claimUIDs, removed := cp.podState.RemoveContainerState(types.UID(pod.GetUid()), ctr.GetName(), types.UID(ctr.GetId()))
	if !removed {
		logger.V(2).Info("ignoring stale or unknown RemoveContainer event")
		return nil
	}
	if len(claimUIDs) > 0 {
		// this serves only for debugging purposes. We should never get here
		updates, err := sharedContainerUpdatesFor(logger, cp.allocationState, cp.podState, types.UID(ctr.GetId()))
		if err != nil {
			logger.Error(err, "unable to calculate shared container updates after RemoveContainer")
		} else {
			logger.Info("RemoveContainer spurious updates needed (unexpected, please file a bug)", "updates", updates)
		}
	}

	// leverage the fact the hook can't fail in our current design
	// **NOTE** because of the flow (see comment before StopContainer) this metric becomes a signal
	// we leaked state and StopContainer didn't clean up properly.
	cp.metrics.RecordNRIRemoveContainer(nil, allocationOf(cp.allocationState, wholeClaimRefs(claimUIDs)), time.Since(startTime))
	return nil
}
