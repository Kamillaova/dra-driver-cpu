# CCX-Aligned Scheduling: Model, Contract, and Lifecycle

> [!NOTE]
> This page describes the end-to-end architecture across the `dra-driver-cpu` driver fork
> and the `scheduler-plugins` `CCXAlign` plugin fork. See [Fork notes](../../FORK.md).

CCX-aligned scheduling guarantees AMD Zen Core Complex (CCX / L3 uncore cache) alignment for
exclusive CPU allocations, eliminating cross-CCX latency penalties for virtual machines and
latency-critical containers. The architecture spans the DRA driver DaemonSet, an out-of-tree
kube-scheduler plugin, and the workload producer.

---

## 1. The Cache-Device Model

Under [`groupBy: uncorecache`](configuration.md#driver-configuration), the driver publishes each physical
L3 uncore cache as a distinct shareable DRA device (`AllowMultipleAllocations: true`). On an AMD EPYC 9554
(2 sockets, 16 CCXs, 16 threads per CCX), the node advertises 16 cache devices per host (8 per NUMA node).

### Published Device Attributes and Taints

Each cache device publishes attributes identifying its geometry, placement, and defragmentation state:

| Attribute / Taint | Key | Type | Description |
|---|---|---|---|
| NUMA Node | `resource.kubernetes.io/numaNode` | `int` | Physical NUMA node ID (0 or 1). Standard KEP-6072 attribute. |
| Physical Size | `dra.cpu/numCPUs` | `int` | Physical number of CPUs in this cache (e.g. `16`). Constant. |
| Cache L3 ID | `dra.cpu/cacheL3ID` | `int` | Kernel L3 cache identifier. |
| Socket ID | `dra.cpu/socketID` | `int` | Physical processor socket ID. |
| Partition | `dra.cpu/partition` | `string` | Name of the CPU partition owning this device (if partitioned). |
| Repair Frontier | `dra.cpu/repairRounds` | `string` | Comma-separated repair rounds for $k \in \{1, 2, 3, 4\}$, e.g. `1,2,3,-`. `-` indicates unreachable. |
| Frontier Input Digest | `dra.cpu/frontierInput` | `string` | Hash of the driver ledger state the frontier was computed against. |
| Partition Taint | `dra.cpu/partition` | Taint (`NoSchedule`) | Present on devices in non-default partitions. Tolerated with exact value match. |
| Floor Taint | `dra.cpu/floor` | Taint (`NoSchedule`) | Present when mirrored capacity is floored at `min + step`. Tolerated by nothing. |
| Poison Taint | `dra.cpu/poisoned` | Taint (`NoSchedule`) | Present when a swap move could not be settled and the NUMA node is fenced. Tolerated by nothing. |

### Claim Shapes

A workload requests CPUs using one of three claim shapes:

1. **Never-Split / Aligned Shape (Exact Count)**:
   A single `requests[].exactly` specification for whole cache(s) or exact sub-cache sizes. No alternative
   subrequests are offered.
   ```yaml
   spec:
     devices:
       requests:
       - name: cpu
         deviceClassName: dra.cpu
         exactly:
           count: 1
           capacity:
             requests:
               dra.cpu/cpu: "16"
   ```
   Because no alternative request exists, the structured parameter allocator cannot split the allocation.

2. **Split-Capable Prioritized List (`firstAvailable`)**:
   Uses Kubernetes prioritized list requests (`DRAPrioritizedList`, GA locked in 1.36). The claim provides
   an ordered list of subrequests from ideal whole-cache alignment down to split alternatives:
   ```yaml
   spec:
     devices:
       requests:
       - name: cpu
         firstAvailable:
         - name: aligned-1
           deviceClassName: dra.cpu
           allocationMode: ExactCount
           count: 1
           capacity:
             requests:
               dra.cpu/cpu: "16"
         - name: split-2
           deviceClassName: dra.cpu
           allocationMode: ExactCount
           count: 2
           capacity:
             requests:
               dra.cpu/cpu: "8"
   ```
   If a single 16-CPU cache is available, the scheduler allocates `aligned-1`. If no clean cache is available,
   the scheduler falls back to `split-2` across two 8-CPU slices, provided the node satisfies the CCXAlign
   repair filter.

3. **Unstructured Exclusive (`bestEffort`)**:
   A single request with `count: N` and per-result capacity of one whole physical core (`min: 2, step: 2` under SMT),
   with no NUMA constraint:
   ```yaml
   spec:
     devices:
       requests:
       - name: cpu
         deviceClassName: dra.cpu
         exactly:
           count: 4
           capacity:
             requests:
               dra.cpu/cpu: "2"
   ```
   The allocator may place the cores on any cache. The defragmenter treats this claim as mobile bystander capacity
   if `relocatable: true` is set.

### Alignment Configuration (`cpuConfig.alignment`)

The claim's opaque configuration controls alignment expectations:

| Value | Meaning | Requirements |
|---|---|---|
| `BestEffort` | Default. Plain exclusive CPUs. Schedulable anywhere; defragmenter may move if `relocatable: true`. | None. |
| `Repairable` | Workload prefers alignment. May start split if the repair frontier is reachable ($1..3$ rounds) and confirmed by a witness at Prepare; runtime defragmentation will consolidate it into a whole cache. | Requires `relocatable: true`. |

> [!IMPORTANT]
> `Strict` is not a valid enum value. Never-split behavior is defined entirely by the claim shape: a claim that
> omits split alternatives in `firstAvailable` cannot be split by the allocator.

### Scoring Modes in CCXAlign v2

The `CCXAlign` v2 plugin scores candidate nodes using fixed codebook bands with 12-point raw margins, ensuring
dominance over general scoring plugins like `NodeResourcesFit` (at weight 10 vs 1):

* **Packing Mode (`MostAllocated`)**: Packs workloads onto warm nodes and occupied caches to preserve clean caches
  for larger tenants.
  - **Warm-Aligned (80..88)**: Aligned demand fits immediately in already tenanted caches.
  - **1-Round Repairable (60..68)**: Split demand repairable in 1 move or swap.
  - **Cold-Aligned / 2-Round Repairable (40..48)**: Clean, untouched caches or 2-round repairable.
  - **3-Round Repairable (20..28)**: Complex 3-round repair.
  - **Split / Unreachable (0..8)**: Feasible only as split without repair promise.
  - Sub-cache reuse bonus adds up to 8 points within each band.

* **Spreading Mode (`LeastAllocated`)**: Spreads tenants across cold nodes and clean caches for maximum thermal
  and L3 bandwidth headroom. Prioritizes cold-aligned nodes (80..96).

### Placement Source: Read the Kernel, Not Files

Workloads must observe placement rules:
* **The kernel is the single source of truth**: A mobile (`relocatable: true`) claim's placement changes at runtime.
  Containers must read their active cpuset from `/proc/self/status` (`Cpus_allowed_list`) or `sched_getaffinity(2)`.
* **Do NOT read KEP-5304 metadata files for active placement**: The downward API metadata file
  (`/var/run/kubernetes.io/dra-device-attributes/.../metadata.json`) is static and written when the container starts.
  It is not updated during defragmentation moves. Reading it for a relocatable claim yields stale placement.
* **Environment variable `DRA_CPUSET_<uid>`**: For relocatable claims, this is set to the literal string `dynamic`.

---

## 2. The Producer Contract

Automated workload controllers (such as `vm-operator`) generating pods and ResourceClaims must conform to these rules:

### 1. Scheduler Name
Pods must specify:
```yaml
spec:
  schedulerName: dracpu-scheduler
```
If omitted, `default-scheduler` schedules the pod without CCX cache scoring, repair frontier verification, or requeuing logic.

### 2. Single Claim Per Container
Each container must reference at most one `dra.cpu` claim. Sharing a claim across multiple containers in the same pod
fails with `CreateContainerError` (`AlreadyOwned`) because the driver's claim store enforces single-owner semantics.

### 3. Claim Shapes per VM Size
For an architecture with 16-CPU uncore caches (e.g. AMD EPYC), producers should use standard templates:

| VM Size | Recommended Claim Shape | Subrequests / Modes | Constraints |
|---|---|---|---|
| **4 vCPU** | Aligned (sub-cache) | `count: 1`, `capacity: 4` | None |
| **8 vCPU** | Aligned (sub-cache) | `count: 1`, `capacity: 8` | None |
| **16 vCPU** | `firstAvailable` | `aligned-1` (1x16) $\to$ `split-2` (2x8) | None |
| **32 vCPU** | `firstAvailable` | `aligned-2` (2x16) $\to$ `split-2` (4x8) | `matchAttribute: resource.kubernetes.io/numaNode` |
| **48 vCPU** | `firstAvailable` | `aligned-3` (3x16) $\to$ `split-2` (6x8) | `matchAttribute: resource.kubernetes.io/numaNode` |
| **64 vCPU** | `firstAvailable` | `aligned-4` (4x16) $\to$ `split-2` (8x8) | `matchAttribute: resource.kubernetes.io/numaNode` |

> [!NOTE]
> For 48 and 64 vCPU VMs, `split-4` (12x4 or 16x4) is physically infeasible inside a single NUMA node on 128-thread
> sockets (where at most 8 CCX devices exist per NUMA node). Only `split-2` alternatives are valid.

### 4. Exact Partition Tolerations
When targeting devices in a named CPU partition, claims must declare an exact toleration:
```yaml
tolerations:
- key: dra.cpu/partition
  operator: Equal
  value: compute
```
Wildcard or empty-key matches (`operator: Exists`) are **strictly prohibited**. An empty-key toleration would bypass
`dra.cpu/floor` (over-stated capacity) and `dra.cpu/poisoned` (fenced NUMA node) taints, landing workloads on damaged devices.

### 5. Quality-of-Service (Guaranteed QoS)
While `DRANodeAllocatableResources` is disabled, the apiserver is blind to DRA allocations. Workloads must mirror the
total claim CPU count into container or pod-level requests and limits:
```yaml
resources:
  requests:
    cpu: "16"
    memory: "32Gi"
  limits:
    cpu: "16"
    memory: "32Gi"
```
This ensures the pod receives Guaranteed QoS and is charged against tenant resource quotas.

### 6. Eviction Protection
Because DRA devices cannot be freed by scheduler preemption without terminating the pod, workload pods should configure:
* A high `priorityClassName` (e.g. `system-cluster-critical` or dedicated VM priority class).
* A `PodDisruptionBudget` (PDB) protecting against voluntary evictions.

---

## 3. Rollout Order and Version Skew

### Readers Before Writers
Components must be rolled out in this exact sequence:
1. **Shared API Module (`api/`)**: Pin the nested `api` module in consumers.
2. **Scheduler Plugin (`scheduler-plugins` / `ccx-align`)**: Deploy the scheduler binary first. The plugin reads
   device attributes (`dra.cpu/repairRounds`, `dra.cpu/frontierInput`, `resource.kubernetes.io/numaNode`, `dra.cpu/numCPUs`).
   It must be able to parse new attributes before the driver begins publishing them.
3. **Driver DaemonSet (`dra-driver-cpu`)**: Deploy the driver DaemonSet after the scheduler plugin is healthy.

### Attribute Skew Tolerance
The architecture tolerates one version of attribute skew in either direction:
* If the driver publishes `dra.cpu/repairRounds` and the plugin is older, the plugin ignores the attribute and evaluates
  clean caches directly.
* If the plugin expects `dra.cpu/repairRounds` and the driver is older or has not computed it, the frontier defaults to
  unreachable (`-`), and split-capable repairable claims fail closed while aligned claims schedule normally.

---

## 4. Rollback Rules

### Disabling the Scheduler Plugin
If scheduling anomalies occur, disable `CCXAlign` in the scheduler profile:
```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
- schedulerName: dracpu-scheduler
  plugins:
    multiPoint:
      enabled:
      - name: CCXAlign
    filter:
      disabled:
      - name: CCXAlign
    score:
      disabled:
      - name: CCXAlign
```
* **Scope**: Disabling the plugin changes new admission and preference only; running pods continue uninterrupted.
* **Driver Mirror Persistence**: Driver mirror obligations remain in force for existing moved claims.
* **Producer Action**: The workload producer must immediately stop submitting repair-dependent claims
  (`alignment: Repairable` with split alternatives) while the plugin is disabled.

### Driver Rollback Safeguards
* **Do NOT downgrade the driver below the capacity mirror while claims sit off their recorded cache**:
  Check the gauge:
  ```
  dra_cpu_claims_off_recorded_cache
  ```
  If this gauge is greater than zero, active claims are running on caches different from their recorded allocations.
  Downgrading to a driver version that lacks the capacity mirror will immediately advertise raw physical capacities,
  causing double-allocation on squatted caches.
* **Downgrade Procedure**: Cordon the node, evict or drain all pods holding `dra.cpu` claims, confirm
  `dra_cpu_claims_off_recorded_cache == 0`, and only then roll back the DaemonSet.

---

## 5. Upgrade and Downgrade Matrix

| Transition | Drain Required? | Prerequisites & Ordering Rules |
|---|---|---|
| **Upstream $\to$ Fork (no defrag)** | **No** | Upstream CDI env values are preserved. Specs upgraded at next Prepare. |
| **Fork (no defrag) $\to$ Fork (defrag)** | **No** | Requires containerd $\ge$ v2.4.0-beta.0 or patched containerd (nri#301 fix) on the node before enabling `defragEnabled: true`. |
| **Fork (cache devices) $\to$ Fork (numa/machine)** | **YES** | Downgrading `groupBy` from `uncorecache` to `numanode` or `machine` requires a full node drain. Existing claims bound to cache devices cannot be interpreted by a node-grouped driver. |
| **Fork (mirror) $\to$ Fork (pre-mirror)** | **YES** (conditional) | Required if `dra_cpu_claims_off_recorded_cache > 0`. Drain node or wait for moved claims to exit before downgrading. |
| **Fork (mutable placement) $\to$ Upstream** | **YES** | Required. Upstream driver cannot parse `dynamic` cpuset env values and will misallocate CPUs. |
| **Container Runtime Upgrade** | **Drain or gate off** | If a host OS upgrade replaces patched containerd with an unpatched binary, `defragEnabled` must be set to `false` or the node drained before upgrading containerd. Running defragmentation against unpatched NRI deadlocks container updates. |

---

## 6. Three-Surface Naming Conventions

Naming conventions across the three configuration surfaces follow specific casing and formatting rules:

| Concept | 1. Scheduler Plugin (`apis/config/types.go`) | 2. Claim Opaque Config (`api/v1alpha1`) | 3. Driver Config / Flags (`internal/driverconfig`) | Device Attribute / Taint |
|---|---|---|---|---|
| **Scoring Strategy** | `scoringStrategy.type: MostAllocated` or `LeastAllocated` | — | `cachePlacementStrategy: pack` or `spread` | — |
| **Alignment Mode** | Evaluates `repairRounds` vs `cleanCaches` | `alignment: BestEffort` or `Repairable` | — | — |
| **Relocatability** | Evaluates claim mobility | `relocatable: true` or `false` | — | — |
| **Grouping Mode** | — | — | `groupBy: uncorecache` | — |
| **Partition Roles** | — | — | `role: reserved`, `shared`, `exclusive` | `dra.cpu/partition: <name>` |
| **Repair Frontier** | Reads `dra.cpu/repairRounds` | — | Publishes frontier on exclusive devices | `dra.cpu/repairRounds: "1,2,3,-"` |
| **Frontier Digest** | Verifies `dra.cpu/frontierInput` | — | Hashes store state into digest | `dra.cpu/frontierInput: "<hash>"` |
| **Defragmentation** | — | — | `defragEnabled: true`, `defragAllowTransientOverlap: true` | — |
| **Taints** | Evaluates taints | Exact tolerations | Manages device taints | `dra.cpu/floor`, `dra.cpu/poisoned` |

* **Surface 1 (Scheduler Plugin)**: UpperCamelCase for option values (`MostAllocated`, `LeastAllocated`), lowerCamelCase for fields.
* **Surface 2 (Claim Opaque Config)**: UpperCamelCase for values (`BestEffort`, `Repairable`), lowerCamelCase for fields (`alignment`, `relocatable`).
* **Surface 3 (Driver Config File)**: lowercase words for enum values (`uncorecache`, `pack`, `spread`, `reserved`, `shared`, `exclusive`), camelCase for YAML/JSON keys.
* **Device Attributes & Taints**: Standard domain prefixes (`dra.cpu/`, `resource.kubernetes.io/`) with C-identifier attribute names.
