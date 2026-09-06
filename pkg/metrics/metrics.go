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

package metrics

import (
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const stability = "ALPHA"

// Result is the bounded label domain for operation result metrics.
type Result string

const (
	ResultSuccess Result = "success"
	ResultError   Result = "error"
	ResultUnknown Result = "unknown"
)

func (r Result) String() string {
	switch r {
	case ResultSuccess:
		return string(ResultSuccess)
	case ResultError:
		return string(ResultError)
	case ResultUnknown:
		return string(ResultUnknown)
	default:
		return string(ResultUnknown)
	}
}

// Descriptor describes a custom driver metric for programmatic introspection.
type Descriptor struct {
	Name      string   `json:"name"`
	Type      string   `json:"type"`
	Stability string   `json:"stability"`
	Help      string   `json:"help"`
	Labels    []string `json:"labels"`
}

// AllocationState is the allocation state represented by CPU allocation gauges.
type AllocationState struct {
	AllocatedCPUs        int
	AvailableCPUs        int
	ReservedCPUs         int
	ActiveResourceClaims int
}

// DefragState is the fragmentation a defragmentation pass observed.
type DefragState struct {
	// ExcessUncoreCaches is how many uncore caches the node's claims span beyond
	// the fewest their sizes allow. Zero means every claim is as well placed as
	// it can be.
	ExcessUncoreCaches int
	// LargestAlignableFreeCPUs is, per NUMA node, the most CPUs still free inside
	// a single uncore cache: the largest claim that node could take without
	// splitting it. It leads the excess count, since it says whether the next
	// claim will land aligned rather than whether the last ones did.
	LargestAlignableFreeCPUs map[int]int
}

// Recorder records driver metrics.
type Recorder interface {
	SetAllocationState(AllocationState)
	RecordPrepare(result Result, duration time.Duration)
	RecordUnprepare(result Result)
	RecordClaimAllocatedCPUs(cpus int)
	// CCX-FORK: upstream's Recorder ends above; the defragmentation methods and
	// everything serving them in this file are the fork's.
	SetDefragState(DefragState)
	RecordDefragPass(result Result, duration time.Duration)
	RecordDefragMoves(result Result, count int)
	RecordDefragBlockedMoves(count int)
	RecordSynchronizeSkippedClaim()
	RecordMisplacedClaim()
	SetPartitionState(map[string]bool)
}

// Metrics owns all custom Prometheus collectors for the CPU driver.
type Metrics struct {
	allocatedCPUs        prometheus.Gauge
	availableCPUs        prometheus.Gauge
	reservedCPUs         prometheus.Gauge
	activeResourceClaims prometheus.Gauge
	prepareClaims        *prometheus.CounterVec
	unprepareClaims      *prometheus.CounterVec
	prepareClaimDuration prometheus.Histogram
	claimAllocatedCPUs   prometheus.Histogram

	defragExcessUncoreCaches      prometheus.Gauge
	defragAlignableFreeCPUs       *prometheus.GaugeVec
	defragPasses                  *prometheus.CounterVec
	defragMoves                   *prometheus.CounterVec
	defragBlockedMoves            prometheus.Counter
	defragPassDurationSecondsHist prometheus.Histogram

	synchronizeSkippedClaims prometheus.Counter
	misplacedClaims          prometheus.Counter
	partitionVerified        *prometheus.GaugeVec
}

type metricKind string

const (
	metricGauge     metricKind = "gauge"
	metricCounter   metricKind = "counter"
	metricHistogram metricKind = "histogram"
)

type metricSpec struct {
	name    string
	kind    metricKind
	help    string
	labels  []string
	buckets []float64
}

var (
	allocatedCPUsSpec = metricSpec{
		name: "dra_cpu_allocated_cpus",
		kind: metricGauge,
		help: "Number of CPUs currently allocated to prepared resource claims.",
	}
	availableCPUsSpec = metricSpec{
		name: "dra_cpu_available_cpus",
		kind: metricGauge,
		help: "Number of CPUs available for allocation after reserved and active claim CPUs are excluded.",
	}
	reservedCPUsSpec = metricSpec{
		name: "dra_cpu_reserved_cpus",
		kind: metricGauge,
		help: "Number of CPUs reserved and excluded from DRA management.",
	}
	activeResourceClaimsSpec = metricSpec{
		name: "dra_cpu_resource_claims_active",
		kind: metricGauge,
		help: "Number of resource claims currently recorded as active by the allocation store.",
	}
	prepareClaimsSpec = metricSpec{
		name:   "dra_cpu_prepare_claims_total",
		kind:   metricCounter,
		help:   "Total number of per-claim PrepareResourceClaims results.",
		labels: []string{"result"},
	}
	unprepareClaimsSpec = metricSpec{
		name:   "dra_cpu_unprepare_claims_total",
		kind:   metricCounter,
		help:   "Total number of per-claim UnprepareResourceClaims results.",
		labels: []string{"result"},
	}
	prepareClaimDurationSpec = metricSpec{
		name:    "dra_cpu_prepare_claim_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of per-claim prepare operations in seconds.",
		buckets: prometheus.DefBuckets,
	}
	claimAllocatedCPUsSpec = metricSpec{
		name:    "dra_cpu_claim_allocated_cpus",
		kind:    metricHistogram,
		help:    "Number of CPUs allocated for each newly successful claim allocation.",
		buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
	}
	defragExcessUncoreCachesSpec = metricSpec{
		name: "dra_cpu_defrag_excess_uncore_caches",
		kind: metricGauge,
		help: "Number of uncore caches the node's claims span beyond the fewest their sizes allow.",
	}
	defragAlignableFreeCPUsSpec = metricSpec{
		name:   "dra_cpu_defrag_largest_alignable_free_cpus",
		kind:   metricGauge,
		help:   "Most CPUs still free within a single uncore cache of a NUMA node, which is the largest claim it can take unsplit.",
		labels: []string{"numa_node"},
	}
	defragPassesSpec = metricSpec{
		name:   "dra_cpu_defrag_passes_total",
		kind:   metricCounter,
		help:   "Total number of defragmentation passes by result.",
		labels: []string{"result"},
	}
	defragMovesSpec = metricSpec{
		name:   "dra_cpu_defrag_moves_total",
		kind:   metricCounter,
		help:   "Total number of claim moves attempted by result, where an error is a move the runtime refused and the driver reverted.",
		labels: []string{"result"},
	}
	defragBlockedMovesSpec = metricSpec{
		name: "dra_cpu_defrag_blocked_moves_total",
		kind: metricCounter,
		help: "Total number of moves a better placement called for that a pass could not make, usually because another claim is in the way.",
	}
	defragPassDurationSpec = metricSpec{
		name:    "dra_cpu_defrag_pass_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of defragmentation passes in seconds.",
		buckets: prometheus.DefBuckets,
	}
	synchronizeSkippedClaimsSpec = metricSpec{
		name: "dra_cpu_synchronize_skipped_claims_total",
		kind: metricCounter,
		help: "Total number of claims or containers Synchronize could not adopt from the runtime's reported state, skipped rather than aborting the whole call.",
	}
	partitionVerifiedSpec = metricSpec{
		name:   "dra_cpu_partition_verified",
		kind:   metricGauge,
		help:   "Whether a CPU partition's declaration matches this machine (1) or contradicts it, in which case the partition publishes no devices (0).",
		labels: []string{"partition"},
	}
	misplacedClaimsSpec = metricSpec{
		name: "dra_cpu_misplaced_claims_total",
		kind: metricCounter,
		help: "Total number of restored claims whose CPUs no single CPU partition holds, which is what a partition list edited under a running node looks like.",
	}
)

var metricSpecs = []metricSpec{
	allocatedCPUsSpec,
	availableCPUsSpec,
	reservedCPUsSpec,
	activeResourceClaimsSpec,
	prepareClaimsSpec,
	unprepareClaimsSpec,
	prepareClaimDurationSpec,
	claimAllocatedCPUsSpec,
	defragExcessUncoreCachesSpec,
	defragAlignableFreeCPUsSpec,
	defragPassesSpec,
	defragMovesSpec,
	defragBlockedMovesSpec,
	defragPassDurationSpec,
	synchronizeSkippedClaimsSpec,
	misplacedClaimsSpec,
	partitionVerifiedSpec,
}

// Descriptors returns metadata for custom CPU driver metrics.
func Descriptors() []Descriptor {
	out := make([]Descriptor, len(metricSpecs))
	for i, spec := range metricSpecs {
		out[i] = Descriptor{
			Name:      spec.name,
			Type:      string(spec.kind),
			Stability: stability,
			Help:      spec.help,
			Labels:    append([]string{}, spec.labels...),
		}
	}
	return out
}

// WriteJSON writes custom metric metadata as deterministic JSON.
func WriteJSON(w io.Writer) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(Descriptors())
}

// New creates and registers the CPU driver custom metrics.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{
		allocatedCPUs:        newGauge(allocatedCPUsSpec),
		availableCPUs:        newGauge(availableCPUsSpec),
		reservedCPUs:         newGauge(reservedCPUsSpec),
		activeResourceClaims: newGauge(activeResourceClaimsSpec),
		prepareClaims:        newCounterVec(prepareClaimsSpec),
		unprepareClaims:      newCounterVec(unprepareClaimsSpec),
		prepareClaimDuration: newHistogram(prepareClaimDurationSpec),
		claimAllocatedCPUs:   newHistogram(claimAllocatedCPUsSpec),

		defragExcessUncoreCaches:      newGauge(defragExcessUncoreCachesSpec),
		defragAlignableFreeCPUs:       newGaugeVec(defragAlignableFreeCPUsSpec),
		defragPasses:                  newCounterVec(defragPassesSpec),
		defragMoves:                   newCounterVec(defragMovesSpec),
		defragBlockedMoves:            newCounter(defragBlockedMovesSpec),
		defragPassDurationSecondsHist: newHistogram(defragPassDurationSpec),

		synchronizeSkippedClaims: newCounter(synchronizeSkippedClaimsSpec),
		misplacedClaims:          newCounter(misplacedClaimsSpec),
		partitionVerified:        newGaugeVec(partitionVerifiedSpec),
	}

	reg.MustRegister(
		m.allocatedCPUs,
		m.availableCPUs,
		m.reservedCPUs,
		m.activeResourceClaims,
		m.prepareClaims,
		m.unprepareClaims,
		m.prepareClaimDuration,
		m.claimAllocatedCPUs,
		m.defragExcessUncoreCaches,
		m.defragAlignableFreeCPUs,
		m.defragPasses,
		m.defragMoves,
		m.defragBlockedMoves,
		m.defragPassDurationSecondsHist,
		m.synchronizeSkippedClaims,
		m.misplacedClaims,
		m.partitionVerified,
	)
	for _, result := range []Result{ResultSuccess, ResultError, ResultUnknown} {
		m.prepareClaims.WithLabelValues(result.String())
		m.unprepareClaims.WithLabelValues(result.String())
		m.defragPasses.WithLabelValues(result.String())
		m.defragMoves.WithLabelValues(result.String())
	}
	return m
}

func newGauge(spec metricSpec) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Name: spec.name,
		Help: spec.help,
	})
}

func newGaugeVec(spec metricSpec) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: spec.name,
		Help: spec.help,
	}, spec.labels)
}

func newCounter(spec metricSpec) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Name: spec.name,
		Help: spec.help,
	})
}

func newCounterVec(spec metricSpec) *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: spec.name,
		Help: spec.help,
	}, spec.labels)
}

func newHistogram(spec metricSpec) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    spec.name,
		Help:    spec.help,
		Buckets: spec.buckets,
	})
}

func (m *Metrics) SetAllocationState(state AllocationState) {
	m.allocatedCPUs.Set(float64(state.AllocatedCPUs))
	m.availableCPUs.Set(float64(state.AvailableCPUs))
	m.reservedCPUs.Set(float64(state.ReservedCPUs))
	m.activeResourceClaims.Set(float64(state.ActiveResourceClaims))
}

func (m *Metrics) RecordPrepare(result Result, duration time.Duration) {
	m.prepareClaims.WithLabelValues(result.String()).Inc()
	m.prepareClaimDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordUnprepare(result Result) {
	m.unprepareClaims.WithLabelValues(result.String()).Inc()
}

func (m *Metrics) RecordClaimAllocatedCPUs(cpus int) {
	m.claimAllocatedCPUs.Observe(float64(cpus))
}

// SetDefragState replaces the per-NUMA-node series wholesale, so a node a pass
// could not measure this time reports nothing rather than its last value.
func (m *Metrics) SetDefragState(state DefragState) {
	m.defragExcessUncoreCaches.Set(float64(state.ExcessUncoreCaches))
	m.defragAlignableFreeCPUs.Reset()
	for numaNodeID, cpus := range state.LargestAlignableFreeCPUs {
		m.defragAlignableFreeCPUs.WithLabelValues(strconv.Itoa(numaNodeID)).Set(float64(cpus))
	}
}

func (m *Metrics) RecordDefragPass(result Result, duration time.Duration) {
	m.defragPasses.WithLabelValues(result.String()).Inc()
	m.defragPassDurationSecondsHist.Observe(duration.Seconds())
}

func (m *Metrics) RecordDefragMoves(result Result, count int) {
	if count <= 0 {
		return
	}
	m.defragMoves.WithLabelValues(result.String()).Add(float64(count))
}

func (m *Metrics) RecordDefragBlockedMoves(count int) {
	if count <= 0 {
		return
	}
	m.defragBlockedMoves.Add(float64(count))
}

func (m *Metrics) RecordSynchronizeSkippedClaim() {
	m.synchronizeSkippedClaims.Inc()
}

func (m *Metrics) RecordMisplacedClaim() {
	m.misplacedClaims.Inc()
}

// SetPartitionState replaces the per-partition series wholesale, so a partition
// a later configuration no longer declares stops reporting rather than keeping
// its last value.
func (m *Metrics) SetPartitionState(verified map[string]bool) {
	m.partitionVerified.Reset()
	for partition, ok := range verified {
		value := 0.0
		if ok {
			value = 1.0
		}
		m.partitionVerified.WithLabelValues(partition).Set(value)
	}
}

type noopRecorder struct{}

// Noop returns a recorder that discards all metric observations.
func Noop() Recorder {
	return noopRecorder{}
}

func (noopRecorder) SetAllocationState(AllocationState)     {}
func (noopRecorder) RecordPrepare(Result, time.Duration)    {}
func (noopRecorder) RecordUnprepare(Result)                 {}
func (noopRecorder) RecordClaimAllocatedCPUs(int)           {}
func (noopRecorder) SetDefragState(DefragState)             {}
func (noopRecorder) RecordDefragPass(Result, time.Duration) {}
func (noopRecorder) RecordDefragMoves(Result, int)          {}
func (noopRecorder) RecordDefragBlockedMoves(int)           {}
func (noopRecorder) RecordSynchronizeSkippedClaim()         {}
func (noopRecorder) RecordMisplacedClaim()                  {}
func (noopRecorder) SetPartitionState(map[string]bool)      {}
