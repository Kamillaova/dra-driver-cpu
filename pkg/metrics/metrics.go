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
	// ResultRewriteError is a threaded group whose CPUs the driver failed to
	// rewrite after an exclusive request took or gave back CPUs.
	ResultRewriteError Result = "rewrite_error"
	// ResultRepairError is a running threaded container whose groups the
	// driver failed to find or repair when it synchronized with the runtime.
	ResultRepairError Result = "repair_error"
)

func (r Result) String() string {
	switch r {
	case ResultSuccess:
		return string(ResultSuccess)
	case ResultError:
		return string(ResultError)
	case ResultUnknown:
		return string(ResultUnknown)
	case ResultRewriteError:
		return string(ResultRewriteError)
	case ResultRepairError:
		return string(ResultRepairError)
	default:
		return string(ResultUnknown)
	}
}

type CPUAllocation string

const (
	CPUAllocationUnknown   CPUAllocation = "unknown"
	CPUAllocationShared    CPUAllocation = "shared"
	CPUAllocationExclusive CPUAllocation = "exclusive"
)

func (c CPUAllocation) String() string {
	switch c {
	case CPUAllocationShared:
		return string(CPUAllocationShared)
	case CPUAllocationExclusive:
		return string(CPUAllocationExclusive)
	case CPUAllocationUnknown:
		return string(CPUAllocationUnknown)
	default:
		return string(CPUAllocationUnknown)
	}
}

// Rule is the bounded label domain for the rules on a claim's shape a
// container may break because they were not applied when it was created.
type Rule string

const (
	// RulePartialHolder refuses a container that names some but not all of a
	// claim's requests.
	RulePartialHolder Rule = "partial_holder"
	// RuleLimitWithOthers refuses a container that holds a request with a
	// limit together with another request.
	RuleLimitWithOthers Rule = "limit_with_others"
	// RuleUnthreadedWithThreaded refuses a container that holds a claim that is
	// not threaded beside a threaded one.
	RuleUnthreadedWithThreaded Rule = "unthreaded_with_threaded"
	// RuleDuplicateGroupName refuses a container whose threaded claims give two
	// groups one name.
	RuleDuplicateGroupName Rule = "duplicate_group_name"
	// RuleDuplicatePlace refuses a container whose threaded claims both set
	// place.
	RuleDuplicatePlace Rule = "duplicate_place"
)

var rules = []Rule{RulePartialHolder, RuleLimitWithOthers, RuleUnthreadedWithThreaded, RuleDuplicateGroupName, RuleDuplicatePlace}

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

// Metrics owns all custom Prometheus collectors for the CPU driver.
type Metrics struct {
	allocatedCPUs              prometheus.Gauge
	availableCPUs              prometheus.Gauge
	reservedCPUs               prometheus.Gauge
	activeResourceClaims       prometheus.Gauge
	prepareClaims              *prometheus.CounterVec
	unprepareClaims            *prometheus.CounterVec
	prepareClaimDuration       prometheus.Histogram
	unprepareClaimDuration     prometheus.Histogram
	claimAllocatedCPUs         prometheus.Histogram
	nriSynchronizeDuration     *prometheus.HistogramVec
	nriCreateContainerDuration *prometheus.HistogramVec
	nriStopContainerDuration   *prometheus.HistogramVec
	nriRemoveContainerDuration *prometheus.HistogramVec
	nriSynchronizeRefused      prometheus.Counter
	misplacedClaims            prometheus.Counter
	containersKeptUnderOlder   *prometheus.CounterVec
	watchdogStuck              prometheus.Gauge
	mappingDropped             prometheus.Counter
	partitionVerified          *prometheus.GaugeVec
	nodeReservationOK          prometheus.Gauge
	deviceTaintsDropped        prometheus.Counter
	threadedContainers         *prometheus.CounterVec
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

// nriBuckets are the histogram buckets for NRI hook duration.
// - 100us-1ms: expected steady state of in-memory operations. We expect the vast majority of samples to land here.
// - 2.5ms-50ms: estimated range for Synchronize call with a non-trivial container count.
// - 100ms-500ms: heavy load for any hook, would require further investigation.
// - 1s, 1.5s, 2s: coarser and coarser before the hard deadline, precursor of hard disconnect which we hit if we cross the hard deadline.
// - 5s: overflow bucket for pathological cases. Tracks "how bad it was".
var nriBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 5}

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
	unprepareClaimDurationSpec = metricSpec{
		name:    "dra_cpu_unprepare_claim_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of per-claim unprepare operations in seconds.",
		buckets: prometheus.DefBuckets,
	}
	claimAllocatedCPUsSpec = metricSpec{
		name:    "dra_cpu_claim_allocated_cpus",
		kind:    metricHistogram,
		help:    "Number of CPUs allocated for each newly successful claim allocation.",
		buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
	}
	nriSynchronizeDurationSpec = metricSpec{
		name:    "dra_cpu_nri_synchronize_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of the NRI Synchronize callbacks in seconds.",
		labels:  []string{"result"},
		buckets: nriBuckets,
	}
	nriCreateContainerDurationSpec = metricSpec{
		name:    "dra_cpu_nri_create_container_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of the NRI CreateContainer callbacks in seconds.",
		labels:  []string{"result", "cpu_allocation_mode"},
		buckets: nriBuckets,
	}
	nriStopContainerDurationSpec = metricSpec{
		name:    "dra_cpu_nri_stop_container_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of the NRI StopContainer callbacks in seconds.",
		labels:  []string{"result", "cpu_allocation_mode"},
		buckets: nriBuckets,
	}
	nriRemoveContainerDurationSpec = metricSpec{
		name:    "dra_cpu_nri_remove_container_duration_seconds",
		kind:    metricHistogram,
		help:    "Duration of the NRI RemoveContainer callbacks in seconds.",
		labels:  []string{"result", "cpu_allocation_mode"},
		buckets: nriBuckets,
	}
	nriSynchronizeRefusedSpec = metricSpec{
		name: "dra_cpu_nri_synchronize_refused_claims_total",
		kind: metricCounter,
		help: "Total number of claims a container names that the NRI Synchronize callback did not bind to it.",
	}
	misplacedClaimsSpec = metricSpec{
		name: "dra_cpu_misplaced_claims_total",
		kind: metricCounter,
		help: "Total number of claims restored at the driver's start with a request whose CPUs lie outside the partition of its device, or whose device the node no longer publishes.",
	}
	containersKeptUnderOlderRuleSpec = metricSpec{
		name:   "dra_cpu_containers_kept_under_older_rule_total",
		kind:   metricCounter,
		help:   "Total number of listed containers the NRI Synchronize callback kept bound to their claims although they break a rule on the claim's shape that was not applied when they were created.",
		labels: []string{"rule"},
	}
	watchdogStuckSpec = metricSpec{
		name: "dra_cpu_watchdog_stuck_seconds",
		kind: metricGauge,
		help: "Seconds the driver's state lock has been held or an NRI hook or Prepare or Unprepare call has run, while one passes 60 seconds; 0 otherwise.",
	}
	mappingDroppedSpec = metricSpec{
		name: "dra_cpu_node_allocatable_mapping_dropped_total",
		kind: metricCounter,
		help: "Total number of ResourceSlices the API server stored without the nodeAllocatableResources mapping the driver published.",
	}
	partitionVerifiedSpec = metricSpec{
		name:   "dra_cpu_partition_verified",
		kind:   metricGauge,
		help:   "Whether the threads per core a CPU partition's smt declares are what the kernel has online (1), or the kernel contradicts them and the partition publishes no devices, or tainted ones under taintDegradedPartitions (0).",
		labels: []string{"partition"},
	}
	deviceTaintsDroppedSpec = metricSpec{
		name: "dra_cpu_device_taints_dropped_total",
		kind: metricCounter,
		help: "Total number of times the driver withheld the devices of degraded CPU partitions because the API server dropped their taint.",
	}
	nodeReservationOKSpec = metricSpec{
		name: "dra_cpu_node_reservation_ok",
		kind: metricGauge,
		help: "Whether the kubelet reserves the CPUs the node's partitions keep from pods without claims (1), or reserves fewer, so the node publishes no devices and refuses every Prepare (0).",
	}
	threadedContainersSpec = metricSpec{
		name:   "dra_cpu_threaded_containers_total",
		kind:   metricCounter,
		help:   "Total number of threaded containers whose groups the NRI StartContainer callback built (success) or failed to build, refusing the start (error), of threaded groups whose CPUs the driver failed to rewrite (rewrite_error), and of running threaded containers whose groups the NRI Synchronize callback failed to find or repair (repair_error).",
		labels: []string{"mode", "result"},
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
	unprepareClaimDurationSpec,
	claimAllocatedCPUsSpec,
	nriSynchronizeDurationSpec,
	nriCreateContainerDurationSpec,
	nriStopContainerDurationSpec,
	nriRemoveContainerDurationSpec,
	nriSynchronizeRefusedSpec,
	containersKeptUnderOlderRuleSpec,
	watchdogStuckSpec,
	mappingDroppedSpec,
	misplacedClaimsSpec,
	partitionVerifiedSpec,
	nodeReservationOKSpec,
	deviceTaintsDroppedSpec,
	threadedContainersSpec,
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
		allocatedCPUs:              newGauge(allocatedCPUsSpec),
		availableCPUs:              newGauge(availableCPUsSpec),
		reservedCPUs:               newGauge(reservedCPUsSpec),
		activeResourceClaims:       newGauge(activeResourceClaimsSpec),
		prepareClaims:              newCounterVec(prepareClaimsSpec),
		unprepareClaims:            newCounterVec(unprepareClaimsSpec),
		prepareClaimDuration:       newHistogram(prepareClaimDurationSpec),
		unprepareClaimDuration:     newHistogram(unprepareClaimDurationSpec),
		claimAllocatedCPUs:         newHistogram(claimAllocatedCPUsSpec),
		nriSynchronizeDuration:     newHistogramVec(nriSynchronizeDurationSpec),
		nriCreateContainerDuration: newHistogramVec(nriCreateContainerDurationSpec),
		nriStopContainerDuration:   newHistogramVec(nriStopContainerDurationSpec),
		nriRemoveContainerDuration: newHistogramVec(nriRemoveContainerDurationSpec),
		nriSynchronizeRefused:      newCounter(nriSynchronizeRefusedSpec),
		containersKeptUnderOlder:   newCounterVec(containersKeptUnderOlderRuleSpec),
		watchdogStuck:              newGauge(watchdogStuckSpec),
		mappingDropped:             newCounter(mappingDroppedSpec),
		misplacedClaims:            newCounter(misplacedClaimsSpec),
		partitionVerified:          newGaugeVec(partitionVerifiedSpec),
		nodeReservationOK:          newGauge(nodeReservationOKSpec),
		deviceTaintsDropped:        newCounter(deviceTaintsDroppedSpec),
		threadedContainers:         newCounterVec(threadedContainersSpec),
	}

	reg.MustRegister(
		m.allocatedCPUs,
		m.availableCPUs,
		m.reservedCPUs,
		m.activeResourceClaims,
		m.prepareClaims,
		m.unprepareClaims,
		m.prepareClaimDuration,
		m.unprepareClaimDuration,
		m.claimAllocatedCPUs,
		m.nriSynchronizeDuration,
		m.nriCreateContainerDuration,
		m.nriStopContainerDuration,
		m.nriRemoveContainerDuration,
		m.nriSynchronizeRefused,
		m.containersKeptUnderOlder,
		m.watchdogStuck,
		m.mappingDropped,
		m.misplacedClaims,
		m.partitionVerified,
		m.nodeReservationOK,
		m.deviceTaintsDropped,
		m.threadedContainers,
	)
	for _, rule := range rules {
		m.containersKeptUnderOlder.WithLabelValues(string(rule))
	}
	for _, result := range []Result{ResultSuccess, ResultError, ResultUnknown} {
		m.prepareClaims.WithLabelValues(result.String())
		m.unprepareClaims.WithLabelValues(result.String())
		m.nriSynchronizeDuration.WithLabelValues(result.String())
	}
	for _, result := range []Result{ResultSuccess, ResultError, ResultUnknown} {
		for _, alloc := range []CPUAllocation{CPUAllocationShared, CPUAllocationExclusive} {
			m.nriCreateContainerDuration.WithLabelValues(result.String(), alloc.String())
			m.nriStopContainerDuration.WithLabelValues(result.String(), alloc.String())
			m.nriRemoveContainerDuration.WithLabelValues(result.String(), alloc.String())
		}
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

func newHistogramVec(spec metricSpec) *prometheus.HistogramVec {
	return prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    spec.name,
		Help:    spec.help,
		Buckets: spec.buckets,
	}, spec.labels)
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

func (m *Metrics) RecordUnprepare(result Result, duration time.Duration) {
	m.unprepareClaims.WithLabelValues(result.String()).Inc()
	m.unprepareClaimDuration.Observe(duration.Seconds())
}

func (m *Metrics) RecordClaimAllocatedCPUs(cpus int) {
	m.claimAllocatedCPUs.Observe(float64(cpus))
}

func (m *Metrics) RecordNRISynchronize(err error, elapsed time.Duration) {
	res := determineResult(err)
	m.nriSynchronizeDuration.WithLabelValues(res.String()).Observe(elapsed.Seconds())
}

func (m *Metrics) RecordNRICreateContainer(err error, alloc CPUAllocation, elapsed time.Duration) {
	res := determineResult(err)
	m.nriCreateContainerDuration.WithLabelValues(res.String(), alloc.String()).Observe(elapsed.Seconds())
}

func (m *Metrics) RecordNRIStopContainer(err error, alloc CPUAllocation, elapsed time.Duration) {
	res := determineResult(err)
	m.nriStopContainerDuration.WithLabelValues(res.String(), alloc.String()).Observe(elapsed.Seconds())
}

func (m *Metrics) RecordNRIRemoveContainer(err error, alloc CPUAllocation, elapsed time.Duration) {
	res := determineResult(err)
	m.nriRemoveContainerDuration.WithLabelValues(res.String(), alloc.String()).Observe(elapsed.Seconds())
}

func (m *Metrics) RecordNRISynchronizeRefusedClaim() {
	m.nriSynchronizeRefused.Inc()
}

func (m *Metrics) RecordMisplacedClaim() {
	m.misplacedClaims.Inc()
}

func (m *Metrics) RecordContainerKeptUnderOlderRule(rule Rule) {
	m.containersKeptUnderOlder.WithLabelValues(string(rule)).Inc()
}

func (m *Metrics) SetWatchdogStuck(stalled time.Duration) {
	m.watchdogStuck.Set(stalled.Seconds())
}

func (m *Metrics) RecordNodeAllocatableMappingDropped() {
	m.mappingDropped.Inc()
}

func (m *Metrics) SetPartitionVerified(partition string, verified bool) {
	value := 0.0
	if verified {
		value = 1
	}
	m.partitionVerified.WithLabelValues(partition).Set(value)
}

func (m *Metrics) SetNodeReservationOK(ok bool) {
	value := 0.0
	if ok {
		value = 1
	}
	m.nodeReservationOK.Set(value)
}

func (m *Metrics) RecordDeviceTaintsDropped() {
	m.deviceTaintsDropped.Inc()
}

func (m *Metrics) RecordThreadedContainer(mode string, result Result) {
	m.threadedContainers.WithLabelValues(mode, result.String()).Inc()
}

func determineResult(err error) Result {
	if err != nil {
		return ResultError
	}
	return ResultSuccess
}

type noopRecorder struct{}

// Noop returns a recorder that discards all metric observations.
func Noop() noopRecorder {
	return noopRecorder{}
}

func (noopRecorder) SetAllocationState(AllocationState)                           {}
func (noopRecorder) RecordPrepare(Result, time.Duration)                          {}
func (noopRecorder) RecordUnprepare(Result, time.Duration)                        {}
func (noopRecorder) RecordClaimAllocatedCPUs(int)                                 {}
func (noopRecorder) RecordNRISynchronize(error, time.Duration)                    {}
func (noopRecorder) RecordNRICreateContainer(error, CPUAllocation, time.Duration) {}
func (noopRecorder) RecordNRIStopContainer(error, CPUAllocation, time.Duration)   {}
func (noopRecorder) RecordNRIRemoveContainer(error, CPUAllocation, time.Duration) {}
func (noopRecorder) RecordNRISynchronizeRefusedClaim()                            {}
func (noopRecorder) RecordMisplacedClaim()                                        {}
func (noopRecorder) RecordContainerKeptUnderOlderRule(Rule)                       {}
func (noopRecorder) SetWatchdogStuck(time.Duration)                               {}
func (noopRecorder) RecordNodeAllocatableMappingDropped()                         {}
func (noopRecorder) SetPartitionVerified(string, bool)                            {}
func (noopRecorder) SetNodeReservationOK(bool)                                    {}
func (noopRecorder) RecordDeviceTaintsDropped()                                   {}
func (noopRecorder) RecordThreadedContainer(string, Result)                       {}
