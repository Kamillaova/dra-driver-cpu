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

// Package admission judges a ResourceClaim's spec by the rules of the
// driver's opaque configuration.
package admission

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	allocationapi "github.com/kubernetes-sigs/dra-driver-cpu/api/allocation"
	"github.com/kubernetes-sigs/dra-driver-cpu/api/allocation/v1alpha1"
	"github.com/kubernetes-sigs/dra-driver-cpu/internal/driverconfig"
	"github.com/kubernetes-sigs/dra-driver-cpu/pkg/device"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
)

// Kind is what a request's devices give it: a share of CPUs, CPUs of its own,
// or both where its results land on devices of either.
type Kind struct {
	Shared bool
	Owned  bool
	// Exclusive says, in a refusal, what makes an owned request exclusive.
	Exclusive string
	// OwnedClass names, in a refusal of `cpuConfig.exclusive: false`, the
	// class whose devices are owned where partitions are used.
	OwnedClass string
	// Node names, in the same refusal, the node that runs without
	// partitions.
	Node string
	// FromAllocation is set where the request is known by the devices the
	// node allocated it alone, its class being none the chart renders.
	FromAllocation bool
}

// FlaggedExclusive is the kind of a request on shared devices that sets
// `cpuConfig.exclusive: true`, which takes CPUs of its own from them.
func FlaggedExclusive() Kind {
	return Kind{Owned: true, Exclusive: "exclusive request (`cpuConfig.exclusive: true`)"}
}

// ClassKinds returns the kind of every DeviceClass the chart renders for cfg,
// named as the chart names them, from the partitions of every profile: a
// class's kind is the same on every node of a supported cluster.
func ClassKinds(driverName string, cfg driverconfig.Config) map[string]Kind {
	kinds := map[string]Kind{}
	if cfg.PartitionsUsed() || len(cfg.Profiles) > 0 {
		kinds[driverName] = Kind{Shared: true}
	} else {
		kinds[driverName] = Kind{Owned: true, Exclusive: fmt.Sprintf("exclusive request (class %s, no cpuPartitions)", driverName)}
	}
	lists := [][]driverconfig.CPUPartition{cfg.CPUPartitions}
	for _, profile := range cfg.Profiles {
		lists = append(lists, profile.CPUPartitions)
	}
	for _, list := range lists {
		for _, partition := range list {
			class := driverName + "." + partition.Name
			switch {
			case partition.Name == device.DefaultPartitionName:
			case partition.Role == device.PARTITION_ROLE_DEFAULT:
				kinds[class] = Kind{Shared: true}
			case partition.Role == device.PARTITION_ROLE_EXCLUSIVE:
				kinds[class] = Kind{Owned: true, Exclusive: fmt.Sprintf("exclusive request (class %s)", class), OwnedClass: class}
			}
		}
	}
	return kinds
}

// ParseLimit reads a written limit: LimitMax, or a quantity above zero. Any
// other value is no limit, and ok is false.
func ParseLimit(limit v1alpha1.Limit) (quantity resource.Quantity, isMax, ok bool) {
	if limit == v1alpha1.LimitMax {
		return resource.Quantity{}, true, true
	}
	quantity, err := resource.ParseQuantity(string(limit))
	if err != nil || quantity.Sign() <= 0 {
		return resource.Quantity{}, false, false
	}
	return quantity, false, true
}

// judged is a request, or an alternative of a firstAvailable request, whose
// kind the caller knows.
type judged struct {
	ref       string
	base      string
	kind      Kind
	amount    resource.Quantity
	hasAmount bool
	spread    string
	limit     *v1alpha1.Limit
	exclusive *bool
}

// noun names the request, or the alternative the scheduler may pick.
func (j judged) noun() string {
	if j.ref != j.base {
		return fmt.Sprintf("subrequest %q", j.ref)
	}
	return fmt.Sprintf("request %q", j.ref)
}

type entry struct {
	index    int
	requests []string
	config   allocationapi.Config
}

// reaches applies resourceclaim.ConfigForResult's matching to a claim's own
// configuration: an entry naming no request reaches every one, and one naming
// a request reaches its alternatives.
func (e entry) reaches(ref string) bool {
	return len(e.requests) == 0 || slices.Contains(e.requests, ref) || slices.Contains(e.requests, resourceclaim.BaseRequestRef(ref))
}

// CheckClaim judges the requests and alternatives of spec whose kind kindOf
// knows, by the class each names, and the driver's configuration entries that
// reach them. Each alternative of a firstAvailable request is judged as a
// request, so a claim is refused whichever alternative the scheduler picks.
func CheckClaim(spec *resourceapi.ResourceClaimSpec, driverName string, kindOf func(ref, class string) (Kind, bool)) error {
	requests := judgedRequests(spec, driverName, kindOf)
	if len(requests) == 0 {
		return nil
	}

	entries, err := driverEntries(spec, driverName, requests)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if e.config.Limit == nil {
			continue
		}
		if _, _, ok := ParseLimit(*e.config.Limit); ok {
			continue
		}
		written := string(*e.config.Limit)
		if written == "" {
			written = `""`
		}
		return fmt.Errorf("%s: invalid limit %s", e.target(requests), written)
	}

	for i := range requests {
		var limited []entry
		for _, e := range entries {
			if e.config.Limit != nil && e.reaches(requests[i].ref) {
				limited = append(limited, e)
			}
		}
		if len(limited) > 1 {
			return fmt.Errorf("%s: cpuConfig.limit set by two config entries (#%d and #%d); set it in one entry", requests[i].noun(), limited[0].index, limited[1].index)
		}
		if len(limited) == 1 {
			requests[i].limit = limited[0].config.Limit
		}
	}

	for i := range requests {
		var flagged []entry
		for _, e := range entries {
			if e.config.Exclusive != nil && e.reaches(requests[i].ref) {
				flagged = append(flagged, e)
			}
		}
		if len(flagged) > 1 {
			return fmt.Errorf("%s: cpuConfig.exclusive set by two config entries (#%d and #%d); set it in one entry", requests[i].noun(), flagged[0].index, flagged[1].index)
		}
		if len(flagged) == 1 {
			requests[i].exclusive = flagged[0].config.Exclusive
		}
	}

	threaded, err := threadedEntry(entries, requests)
	if err != nil {
		return err
	}
	if !threaded.IsZero() {
		if err := checkThreadedShape(spec, threaded, requests); err != nil {
			return err
		}
	}
	var groups []threadedGroup
	if threaded.Enabled {
		if groups, err = threadedGroups(spec, threaded, requests); err != nil {
			return err
		}
	}

	for i := range requests {
		if err := checkExclusiveFlag(requests[i], driverName); err != nil {
			return err
		}
		if requests[i].kind.Shared && requests[i].exclusive != nil && *requests[i].exclusive {
			requests[i].kind = FlaggedExclusive()
		}
	}

	for _, j := range requests {
		if err := checkLimit(j); err != nil {
			return err
		}
	}
	if threaded.Enabled {
		for _, g := range groups {
			if err := checkCappedInGroup(g, requests); err != nil {
				return err
			}
		}
		return nil
	}
	for _, j := range requests {
		if err := checkCapped(j, requests); err != nil {
			return err
		}
	}
	return nil
}

// threadedEntry returns the one threaded block of the claim's entries, its
// zero value where none carries one. A block applies to the whole claim, so
// an entry carrying one is unscoped or reaches every judged request; Kubernetes
// defines an entry listing every request to mean the same as one listing
// none. An entry whose block is all zero values is no entry.
func threadedEntry(entries []entry, requests []judged) (v1alpha1.ThreadedConfig, error) {
	var found *entry
	for i := range entries {
		e := &entries[i]
		if e.config.Threaded.IsZero() {
			continue
		}
		if len(e.requests) > 0 && slices.ContainsFunc(requests, func(j judged) bool { return !e.reaches(j.ref) }) {
			return v1alpha1.ThreadedConfig{}, fmt.Errorf("cgroupConfig: applies to the whole claim; drop \"requests\" from its config entry, or list every request (%s)",
				strings.Join(requestBases(requests), ", "))
		}
		if found != nil {
			return v1alpha1.ThreadedConfig{}, fmt.Errorf("cgroupConfig set by two config entries (#%d and #%d); set it in one entry", found.index, e.index)
		}
		found = e
	}
	if found == nil {
		return v1alpha1.ThreadedConfig{}, nil
	}
	return found.config.Threaded, nil
}

// requestBases returns the requests the judged requests and alternatives
// belong to, in the claim's order.
func requestBases(requests []judged) []string {
	var bases []string
	for _, j := range requests {
		if !slices.Contains(bases, j.base) {
			bases = append(bases, j.base)
		}
	}
	return bases
}

// checkThreadedShape refuses a threaded block that is wrong whatever enabled
// says: an empty groups list, a group whose name is no DNS label, is given
// twice or holds no requests, a group naming what is no request or alternative
// of the claim, a name in two groups, a request named beside one of its own
// alternatives, and a place naming no group.
func checkThreadedShape(spec *resourceapi.ResourceClaimSpec, t v1alpha1.ThreadedConfig, requests []judged) error {
	if t.Groups != nil && len(t.Groups) == 0 {
		return errors.New("cgroupConfig.threaded.groups: empty; list the groups, or drop it for one group per request")
	}
	claimRefs := claimRequestRefs(spec)
	listedIn := map[string][]string{}
	for i, g := range t.Groups {
		if errs := validation.IsDNS1123Label(g.Name); len(errs) > 0 {
			return fmt.Errorf("group %q: not a DNS label; use lower-case letters, digits and \"-\"", g.Name)
		}
		if slices.ContainsFunc(t.Groups[:i], func(other v1alpha1.ThreadedGroup) bool { return other.Name == g.Name }) {
			return fmt.Errorf("group %q appears twice in cgroupConfig.threaded.groups; group names must be unique", g.Name)
		}
		if len(g.Requests) == 0 {
			return fmt.Errorf("group %q names no request; name its requests, or drop the group", g.Name)
		}
		for _, name := range g.Requests {
			if !slices.Contains(claimRefs, name) {
				return fmt.Errorf("group %q: the claim has no request %q", g.Name, name)
			}
			if !slices.Contains(listedIn[name], g.Name) {
				listedIn[name] = append(listedIn[name], g.Name)
			}
		}
	}
	for _, g := range t.Groups {
		for _, name := range g.Requests {
			if in := listedIn[name]; len(in) > 1 {
				return fmt.Errorf("%s: in groups %q and %q; a request belongs to one group", refNoun(name), in[0], in[1])
			}
			base := resourceclaim.BaseRequestRef(name)
			if base == name {
				continue
			}
			viaRequest := listedIn[base]
			if len(viaRequest) == 0 {
				continue
			}
			if viaRequest[0] == listedIn[name][0] {
				return fmt.Errorf("group %q names request %q and its alternative %q; name the request or its alternatives, not both", viaRequest[0], base, name)
			}
			return fmt.Errorf("group %q names request %q and group %q its alternative %q; name the request or its alternatives, not both", viaRequest[0], base, listedIn[name][0], name)
		}
	}
	if t.Place == "" {
		return nil
	}
	names := groupNames(t, requests)
	if !slices.Contains(names, t.Place) {
		return fmt.Errorf("cgroupConfig.threaded.place %q: no such group (groups: %s)", t.Place, strings.Join(names, ", "))
	}
	return nil
}

func refNoun(ref string) string {
	if resourceclaim.BaseRequestRef(ref) != ref {
		return fmt.Sprintf("subrequest %q", ref)
	}
	return fmt.Sprintf("request %q", ref)
}

// claimRequestRefs returns the name of every request of the claim and of every
// alternative of its firstAvailable requests, whatever driver serves them.
func claimRequestRefs(spec *resourceapi.ResourceClaimSpec) []string {
	var refs []string
	for _, request := range spec.Devices.Requests {
		refs = append(refs, request.Name)
		for _, sub := range request.FirstAvailable {
			refs = append(refs, resourceclaim.CreateSubRequestRef(request.Name, sub.Name))
		}
	}
	return refs
}

// groupNames returns the names of the block's groups: those it lists, or else
// one per judged request, named after it.
func groupNames(t v1alpha1.ThreadedConfig, requests []judged) []string {
	if t.Groups == nil {
		return requestBases(requests)
	}
	names := make([]string, 0, len(t.Groups))
	for _, g := range t.Groups {
		names = append(names, g.Name)
	}
	return names
}

// GroupOf returns the group of a threaded block that holds a result of the
// request or alternative ref: the group listing ref or its request, or,
// without a groups list, the group named after its request. It returns false
// where no group holds it.
func GroupOf(t v1alpha1.ThreadedConfig, ref string) (string, bool) {
	base := resourceclaim.BaseRequestRef(ref)
	if t.Groups == nil {
		return base, true
	}
	for _, g := range t.Groups {
		if slices.Contains(g.Requests, ref) || slices.Contains(g.Requests, base) {
			return g.Name, true
		}
	}
	return "", false
}

type threadedGroup struct {
	name    string
	members []int
}

// threadedGroups returns the judged requests each group of an enabled block
// holds, refusing a judged request no group holds, whose CPUs would sit in the
// thread root, and a place whose group may receive no result: a group naming
// a request whole, or every alternative of one, receives one wherever the
// request's alternatives are this driver's.
func threadedGroups(spec *resourceapi.ResourceClaimSpec, t v1alpha1.ThreadedConfig, requests []judged) ([]threadedGroup, error) {
	groups := make([]threadedGroup, 0, len(requests))
	for _, name := range groupNames(t, requests) {
		groups = append(groups, threadedGroup{name: name})
	}
	for i, j := range requests {
		name, ok := GroupOf(t, j.ref)
		if !ok {
			return nil, notInAGroup(j)
		}
		g := slices.IndexFunc(groups, func(g threadedGroup) bool { return g.name == name })
		groups[g].members = append(groups[g].members, i)
	}
	if t.Place == "" || t.Groups == nil {
		return groups, nil
	}
	place := t.Groups[slices.IndexFunc(t.Groups, func(g v1alpha1.ThreadedGroup) bool { return g.Name == t.Place })]
	if !namesARequest(spec, place, requests) {
		return nil, fmt.Errorf("cgroupConfig.threaded.place %q: group %q names no request whole nor every alternative of one, and is not built where none of its alternatives is allocated; place a group that names a request",
			t.Place, t.Place)
	}
	return groups, nil
}

func notInAGroup(j judged) error {
	switch {
	case j.kind.FromAllocation:
		return fmt.Errorf("%s is in no group; add it to cgroupConfig.threaded.groups", j.noun())
	case j.ref != j.base:
		return fmt.Errorf("%s: in no group of cgroupConfig.threaded.groups; add it or its request %q to a group, or drop groups for one group per request", j.noun(), j.base)
	}
	return fmt.Errorf("%s: in no group of cgroupConfig.threaded.groups; add it to a group, or drop groups for one group per request", j.noun())
}

// namesARequest reports whether a group names a judged request whole, or every
// alternative of a firstAvailable request.
func namesARequest(spec *resourceapi.ResourceClaimSpec, g v1alpha1.ThreadedGroup, requests []judged) bool {
	for _, request := range spec.Devices.Requests {
		if slices.Contains(g.Requests, request.Name) && slices.ContainsFunc(requests, func(j judged) bool { return j.base == request.Name }) {
			return true
		}
		if len(request.FirstAvailable) > 0 && !slices.ContainsFunc(request.FirstAvailable, func(sub resourceapi.DeviceSubRequest) bool {
			return !slices.Contains(g.Requests, resourceclaim.CreateSubRequestRef(request.Name, sub.Name))
		}) {
			return true
		}
	}
	return false
}

// Threaded judges the claim's threaded block by its shape, its scope and the
// group it gives each judged request, as CheckClaim does, and returns it, its
// zero value where no entry of the driver's carries one.
func Threaded(spec *resourceapi.ResourceClaimSpec, driverName string, kindOf func(ref, class string) (Kind, bool)) (v1alpha1.ThreadedConfig, error) {
	requests := judgedRequests(spec, driverName, kindOf)
	if len(requests) == 0 {
		return v1alpha1.ThreadedConfig{}, nil
	}
	entries, err := driverEntries(spec, driverName, requests)
	if err != nil {
		return v1alpha1.ThreadedConfig{}, err
	}
	threaded, err := threadedEntry(entries, requests)
	if err != nil || threaded.IsZero() {
		return v1alpha1.ThreadedConfig{}, err
	}
	if err := checkThreadedShape(spec, threaded, requests); err != nil {
		return v1alpha1.ThreadedConfig{}, err
	}
	if threaded.Enabled {
		if _, err := threadedGroups(spec, threaded, requests); err != nil {
			return v1alpha1.ThreadedConfig{}, err
		}
	}
	return threaded, nil
}

// driverEntries parses the driver's configuration entries that reach a judged
// request.
func driverEntries(spec *resourceapi.ResourceClaimSpec, driverName string, requests []judged) ([]entry, error) {
	var entries []entry
	for i, config := range spec.Devices.Config {
		if config.Opaque == nil || config.Opaque.Driver != driverName {
			continue
		}
		e := entry{index: i, requests: config.Requests}
		if !slices.ContainsFunc(requests, func(j judged) bool { return e.reaches(j.ref) }) {
			continue
		}
		parsed, err := allocationapi.ParseOpaqueConfig(config.Opaque.Parameters.Raw)
		if err != nil {
			return nil, fmt.Errorf("config entry #%d: %w", i, err)
		}
		e.config = parsed
		entries = append(entries, e)
	}
	return entries, nil
}

// target names, in a refusal of the entry's limit, the first judged request it
// reaches, or the alternative where the entry names that alternative.
func (e entry) target(requests []judged) string {
	for _, j := range requests {
		if !e.reaches(j.ref) {
			continue
		}
		if j.ref != j.base && slices.Contains(e.requests, j.ref) {
			return j.noun()
		}
		return fmt.Sprintf("request %q", j.base)
	}
	return ""
}

func judgedRequests(spec *resourceapi.ResourceClaimSpec, driverName string, kindOf func(ref, class string) (Kind, bool)) []judged {
	var requests []judged
	add := func(ref, base, class string, capacity *resourceapi.CapacityRequirements, mode resourceapi.DeviceAllocationMode, count int64) {
		kind, ok := kindOf(ref, class)
		if !ok {
			return
		}
		j := judged{ref: ref, base: base, kind: kind, spread: spreadOf(mode, count)}
		if capacity != nil {
			j.amount, j.hasAmount = device.LookupConsumedCapacity(capacity.Requests, driverName)
		}
		requests = append(requests, j)
	}
	for _, request := range spec.Devices.Requests {
		if request.Exactly != nil {
			add(request.Name, request.Name, request.Exactly.DeviceClassName, request.Exactly.Capacity, request.Exactly.AllocationMode, request.Exactly.Count)
		}
		for _, sub := range request.FirstAvailable {
			add(resourceclaim.CreateSubRequestRef(request.Name, sub.Name), request.Name, sub.DeviceClassName, sub.Capacity, sub.AllocationMode, sub.Count)
		}
	}
	return requests
}

func spreadOf(mode resourceapi.DeviceAllocationMode, count int64) string {
	switch {
	case mode == resourceapi.DeviceAllocationModeAll:
		return "`allocationMode: All`, which may take several devices"
	case count > 1:
		return fmt.Sprintf("`count: %d`", count)
	}
	return ""
}

// checkExclusiveFlag refuses `cpuConfig.exclusive: false` where the request's
// devices are owned, which give no share to ask for.
func checkExclusiveFlag(j judged, driverName string) error {
	if j.exclusive == nil || *j.exclusive || !j.kind.Owned {
		return nil
	}
	if j.kind.OwnedClass != "" {
		return fmt.Errorf("%s: `cpuConfig.exclusive: false`, but class %s gives exclusive CPUs only; drop cpuConfig.exclusive, or use a shared class such as %s",
			j.noun(), j.kind.OwnedClass, driverName)
	}
	return fmt.Errorf("%s: `cpuConfig.exclusive: false`, but node %q runs without partitions, where %s gives exclusive CPUs only; add `- name: default` to cpuPartitions, or drop cpuConfig.exclusive",
		j.noun(), j.kind.Node, driverName)
}

// checkLimit refuses a numeric limit on owned CPUs, which it would throttle,
// and on a share a limit with no amount or below its amount, as Kubernetes
// refuses a container limit below its request.
func checkLimit(j judged) error {
	if j.limit == nil {
		return nil
	}
	quantity, isMax, _ := ParseLimit(*j.limit)
	if j.kind.Owned && !isMax {
		return fmt.Errorf("request %q: limit %s on %s; only `limit: max` is allowed", j.ref, *j.limit, j.kind.Exclusive)
	}
	if !j.kind.Shared {
		return nil
	}
	if !j.hasAmount {
		return fmt.Errorf("%s: limit without a request amount; add a dra.cpu/cpu capacity request, it can be zero", j.noun())
	}
	if !isMax && quantity.Cmp(j.amount) < 0 {
		return fmt.Errorf("%s: limit %s is below the amount %s", j.noun(), *j.limit, j.amount.String())
	}
	return nil
}

// capOf returns the cap a share asks for, as its refusals name it: its
// numeric limit as written, or else its amount above zero.
func capOf(j judged) (string, bool) {
	if !j.kind.Shared {
		return "", false
	}
	if j.limit != nil {
		return string(*j.limit), *j.limit != v1alpha1.LimitMax
	}
	return j.amount.String(), j.hasAmount && j.amount.Sign() > 0
}

// checkCapped refuses a capped share that may take more than one device, since
// a cgroup has one cpu.max, or that shares the claim with another request of
// this driver, all of which land in the one cgroup of the container using the
// claim. Two alternatives of one request are never beside each other.
func checkCapped(j judged, requests []judged) error {
	limit, capped := capOf(j)
	if !capped {
		return nil
	}
	if j.spread != "" {
		return spreadRefusal(j, limit)
	}
	others := othersBeside(j, requests, nil)
	if len(others) == 1 {
		return fmt.Errorf("%s has limit of %s, so it cannot be used together with request %q: use cgroupConfig.threaded, or remove the limit", j.noun(), limit, others[0])
	}
	if len(others) > 1 {
		return fmt.Errorf("%s has limit of %s, so it cannot be used together with requests %s: use cgroupConfig.threaded, or remove the limit", j.noun(), limit, quoteAll(others))
	}
	return nil
}

// checkCappedInGroup refuses, in a threaded group, a capped share that may
// take more than one device, or that shares the group's cgroup with another
// request.
func checkCappedInGroup(g threadedGroup, requests []judged) error {
	for _, i := range g.members {
		j := requests[i]
		limit, capped := capOf(j)
		if !capped {
			continue
		}
		if j.spread != "" {
			return spreadRefusal(j, limit)
		}
		others := othersBeside(j, requests, g.members)
		if len(others) == 1 {
			return fmt.Errorf("group %q: %s has limit of %s, so it cannot be used together with request %q: give it its own group", g.name, j.noun(), limit, others[0])
		}
		if len(others) > 1 {
			return fmt.Errorf("group %q: %s has limit of %s, so it cannot be used together with requests %s: give it its own group", g.name, j.noun(), limit, quoteAll(others))
		}
	}
	return nil
}

func spreadRefusal(j judged, limit string) error {
	return fmt.Errorf("%s has limit of %s with %s; a limit can be applied to only one device: ask for one device, or remove the limit", j.noun(), limit, j.spread)
}

// othersBeside returns the requests other than j's own that share its cgroup:
// those of members, or of every judged request where members is nil.
func othersBeside(j judged, requests []judged, members []int) []string {
	var others []string
	for i, other := range requests {
		if members != nil && !slices.Contains(members, i) {
			continue
		}
		if other.base != j.base && !slices.Contains(others, other.base) {
			others = append(others, other.base)
		}
	}
	return others
}

func quoteAll(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, strconv.Quote(name))
	}
	return strings.Join(quoted, ", ")
}
