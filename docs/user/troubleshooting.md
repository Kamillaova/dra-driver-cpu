# Troubleshooting & Diagnostics

The repository includes a diagnostic tool available as `dracpu gatherinfo` to collect node-local CPU topology and driver configuration information. The legacy `dracpu-gatherinfo` alias remains supported in versions 0.2.z and 0.3.z. This is helpful for debugging CPU allocation issues or validating that the host topology is discovered correctly.

## `dracpu gatherinfo` Overview

`dracpu gatherinfo` is a node-local diagnostic tool for collecting CPU information from the host where it runs. The command entrypoint is thin; the collection logic lives in `internal/gatherinfo` and writes a YAML report with a top-level `layoutVersion`.

This first iteration intentionally keeps the scope small:

- `cpuDetails`
- `driverConfig`

It does not collect Kubernetes control-plane objects and it does not emit the experimental hierarchical topology view.

## Usage

```bash
./dracpu gatherinfo
```

By default the tool writes a temporary YAML file, prints the final path, and exits.

To place the output under a specific directory:

```bash
./dracpu gatherinfo --output-dir=/var/log/dracpu-debug
```

In that case the tool writes one file per invocation under `/var/log/dracpu-debug`.

To stream the YAML report to standard output instead of creating a file:

```bash
./dracpu gatherinfo --stdout > node-a.yaml
```

`--stdout` is intended for remote collection flows such as `kubectl exec`. It cannot be combined with `--output-dir`.

## Output Format

Example file:

```yaml
---
toolVersion:
  goVersion: go1.26.0
  vcsRevision: abcdef1234567890
  vcsTime: "2026-05-29T03:00:00Z"
layoutVersion: v1
cpuDetails:
  topology:
    numCPUs: 16
    numCores: 8
    numUncoreCache: 1
    numSockets: 1
    numNUMANodes: 1
    smtEnabled: true
  cpus:
    - cpuID: 0
      coreID: 0
      socketID: 0
      clusterID: -1
      numaNodeID: 0
      numaNodeCPUSet: 0-15
      sibling: 8
      coreType: standard
      uncoreCacheID: 0
driverConfig:
  cpuDeviceMode: grouped
  groupBy: numanode
  bindAddress: :8080
  reservedCPUs: 0-3
```

The report records:

- which binary produced the data
- which report layout the consumer should expect through the top-level `layoutVersion`
- the flat CPU inventory from `pkg/cpuinfo`
- the effective driver-side settings for the same node

When the driver process is running in the same container, `driverConfig` is derived from the driver's actual command line and falls back to defaults only when that information is unavailable.

## A Stuck Driver

The driver times how long its state lock has been held and how long each NRI hook and each
Prepare or Unprepare call has been running, and acts on two fixed thresholds:

- Past 60 seconds the driver reports itself stuck: `/healthz` answers 503, so the pod turns not
  ready, `dra_cpu_watchdog_stuck_seconds` shows how long the stall has lasted, and the log holds
  one `driver stuck, reporting not ready` line naming what is stuck, with the stacks of all
  goroutines. Once nothing is past 60 seconds any more, the driver logs `driver no longer stuck`,
  the metric drops back to 0 and `/healthz` answers 200 again.
- Past 180 seconds the driver logs `watchdog stopping the driver`, stops and exits non-zero, and
  the kubelet restarts its container.

The watchdog makes no API call, so an unreachable API server does not set it off. Attach the
`driver stuck` line with its goroutine stacks to a bug report.

## A Container Kept Under an Older Rule

A rule on a claim's shape reaches a container only when the driver applies it at the container's
creation, so a container created before a driver release added the rule, or while the driver was not
connected, keeps breaking it until it is created again. When the driver starts or reconnects to the
container runtime, a running container that breaks such a rule keeps the CPUs of its claims where
the runtime shows it pinned to exactly those CPUs. The driver then posts a `KeptUnderOlderRule`
Warning event on its pod, which names the container, the rule and the claim, and what to change
where its detail gives advice, and counts the container in
`dra_cpu_containers_kept_under_older_rule_total`, whose entry in [metrics](metrics.md) lists the
rules. Change the pod as the event says: when the container is created again, the driver refuses it.

## A Profile Label Without Profiles

A `dra.cpu/profile` label selects a node's profile only where the configuration declares
[profiles](configuration.md#per-node-profiles). Without them, a driver on a node carrying the label
stops before it registers with the kubelet or publishes anything. Its log ends with a
`failed to run` line whose error reads:

```text
driver failed to start: node label dra.cpu/profile=r7625 names a profile, and the config declares no profiles: declare the profiles the nodes' labels name, or remove the label with kubectl label node worker-a dra.cpu/profile-
```

Remove the label, `kubectl label node worker-a dra.cpu/profile-`, or declare the profiles the
labels name. To move a fleet to profiles, change the configuration first, declaring a profile for
each kind of node, then label each node and restart its driver: the driver reads the configuration
and the label only when it starts, and once profiles are declared it refuses a node without the
label.

## A Capped Shared Request

A shared request with a limit, its `cpuConfig.limit` or else an amount above `0`, is capped at it,
and must take one device and be alone in its container's cgroup, see
[Capped Shared Requests](workload-requirements.md#capped-shared-requests). Prepare refuses a claim
that breaks this, and the pod's `FailedPrepareDynamicResources` event carries one of:

```text
request "vmm-cpu" has limit of 250m with `count: 2`; a limit can be applied to only one device: ask for one device, or remove the limit
request "vmm-cpu" has limit of 250m, so it cannot be used together with request "vcpu": remove the limit
```

A container whose claims together put a capped request beside another request shows
`CreateContainerError` with:

```text
container "app": request "cpus" of claim "app-a-x7k2p" has limit of 250m, so it cannot be used together with request "cpus" of claim "app-b-q9m4t": use the claims in separate containers, or remove the limit
```

A container the driver finds running when it starts or reconnects to the container runtime, holding
a capped request beside another request, keeps running without the cap, and gets a
`KeptUnderOlderRule` Warning with the rule `limit_with_others`, see
[A Container Kept Under an Older Rule](#a-container-kept-under-an-older-rule). A running container
whose cap is missing, where the container has no CPU limit, gets it then.

To see the cap a container runs under, read `cpu.max` in its cgroup: `25000 100000` for `250m`, and
`max 100000` for none. A container that shows another quota has a CPU limit, set in its pod or by a
`LimitRange` or another admission controller, which replaces the cap. Such a container gets a
`CPULimitReplacesCap` Warning event on its pod when it is created, naming the container, its CPU
limit and the claim's cap:

```text
container "app": its CPU limit of 500m replaces the limit of 250m of request "cpus" of claim "app-a-x7k2p"
```

The container runs under the kubelet's quota. Remove the CPU limit, or the `LimitRange` default or
the admission controller that sets it, and create the pod again.

## CPUs Held by Another Claim

When the driver cannot prepare a claim because claims it has already prepared hold the CPUs, the
pod's `FailedPrepareDynamicResources` event carries the refusal. It names each claim that holds
them, from the driver's own record and with no API call: its namespace and name where the driver
recorded them, its UID, the CPUs it holds, and the container it is bound to, or `bound to no container`. The list ends with a pointer to this section. The refusal takes one of three forms:

- An overlap, when the claim was allocated CPUs a prepared claim holds, for example
  `claim ns/new has overlapping device assignment with other claims: claim ns/old (UID "...") holds CPUs 0, bound to no container`.
- A shortage, when a device has fewer free CPUs or free whole cores than the scheduler counted:
  `not enough cpus available to satisfy request: requested=2, available=1: the CPUs the scheduler counts free on device "cpudevnuma000" are held by one of these claims: ...`,
  followed by every claim that holds CPUs of that device.
- With `fullPhysicalCPUsOnly`, an odd number of CPUs on a device whose cores differ in threads
  needs a free core of 1 thread:
  `` request "vcpu": 3 exclusive CPUs need a free core of 1 thread on device "cpudevnuma000", which has none (2 free cores of 2 threads; `fullPhysicalCPUsOnly: true`): claims holding the device's cores of 1 thread: ... ``.

The third form can come on a healthy node: the scheduler counts CPUs, not the threads of their
cores, so every claim it lists can be live. A request for an even number of CPUs needs no core of 1
thread.

After an overlap or a shortage, one of the listed claims holds CPUs the scheduler counts free,
since it counts free only the CPUs no allocated claim holds. Look up each listed claim with the API:

- If a listed claim is gone, replaced by a claim of the same name, or allocated to another node,
  the driver keeps the record of a claim the kubelet no longer knows. Free its CPUs as below.
- If every listed claim still exists and is allocated to this node, the scheduler allocated one
  device twice. Kubernetes v1.37.0's scheduler can do so after a failed PreBind, fixed for 1.38 by
  kubernetes#143450. The listed claims are live: do not free their CPUs as below.

The driver frees a claim's CPUs only when the kubelet unprepares the claim, and it restores every
claim from its CDI spec when it starts. Never remove the kubelet's DRA state, its
`dra_manager_state` checkpoint or the kubelet root directory, on a node with prepared claims: drain
the node first, or reboot it before it runs pods again. Otherwise the claims the kubelet forgets
keep their CPUs until the node reboots.

To free the CPUs of a claim the kubelet no longer knows:

1. Find the holding claim in the pod's `FailedPrepareDynamicResources` event.
1. Confirm with the API that the claim is gone, replaced, or allocated to another node, and that
   `dra_cpu_resource_claims_active` on the node is above the number of `dra.cpu` claims allocated
   to the node.
1. Drain the node and reboot it.

Where the node cannot be rebooted, remove the claim's CDI spec,
`/var/run/cdi/dra.k8s.io-cpu_claim-<claim UID>.json`, from a privileged pod on the node, then
delete the driver pod: the new driver pod restores its records without that claim.

## A Stalled Claim Status Write

The driver writes a claim's status at the end of every preparation, within the kubelet's deadline of
45 seconds for the call, and a failed write fails the preparation with:

```text
claim vms/vm-1: status not written: <the API server's error>
```

The kubelet retries the preparation, which keeps the CPUs the claim was given and writes the status
again. While the API server answers reads but stalls writes, each preparation waits up to that
deadline, and the kubelet's plugin library runs one preparation or unpreparation at a time, so the
unpreparations queued behind it can time out at the kubelet too, and are retried. Containers keep
starting meanwhile: the driver's runtime hooks never wait on the write.

## A Node-Allocatable Mapping the API Server Dropped

With `publishNodeAllocatableResourceMapping: true` the driver publishes each device with a
`nodeAllocatableResources` mapping, so the scheduler and the kubelet count the CPUs of `dra.cpu`
claims against the node's `cpu`. An API server with the `DRANodeAllocatableResources` feature
gate off stores the ResourceSlices without the mapping and refuses nothing: those CPUs stop
counting, and ordinary pods can be placed on CPUs that claims already hold. The driver keeps its
devices published and reports the drop in the condition `CPUAllocatableMappingDropped` on its
Node:

```bash
kubectl get node <node> -o jsonpath='{.status.conditions[?(@.type=="CPUAllocatableMappingDropped")]}'
```

- `True`, reason `DroppedByAPIServer`: the API server dropped the mapping since the driver
  started, so `dra.cpu` claims are not counted against the node's `cpu`.
  `dra_cpu_node_allocatable_mapping_dropped_total` counts each ResourceSlice stored without it,
  and the driver logs `API server dropped nodeAllocatableResources from a ResourceSlice`.
- `False`, reason `MappingPublished`: every ResourceSlice kept the mapping since the driver
  started.
- `Unknown`, reason `DriverStopped`: the driver has stopped and checks nothing.

Once the API server has dropped the mapping, the driver stops sending it until it restarts, so
the condition stays `True` after the API server is fixed. Enable `DRANodeAllocatableResources` on
the API server, then restart the driver pod on each node whose condition is `True`.

A kubelet with the gate off is a different case: the API server refuses the node's ResourceSlices
that carry the mapping, so the driver publishes no devices and logs the refusal.

## A CPU Partition Whose SMT the Kernel Contradicts

A partition's `smt` says how many threads per core the platform leaves online on the partition's
cores; the driver only checks it. Where the kernel has more threads online on a partition's cores
than its `smt` declares, the partition is degraded: it publishes no devices, so no claim can take
its CPUs, and the rest of the node serves as usual. With `taintDegradedPartitions` on, it publishes
its devices with a taint instead, so only a claim tolerating the taint can take its CPUs; see
[A degraded partition](cpu-partitions.md#a-degraded-partition). A partition that itself lists more
threads of one core than its `smt` allows is refused at start instead, since taking the extra
threads offline would leave it naming an offline CPU. Where the degraded partition is the `default`
entry, its shared devices are the ones not published, or tainted, and containers without claims keep
running on its CPUs. The driver reports it in the condition `CPUPartitionDegraded` on its Node, on
every node that uses `cpuPartitions`:

```bash
kubectl get node <node> -o jsonpath='{.status.conditions[?(@.type=="CPUPartitionDegraded")]}'
```

- `True`, reason `SiblingThreadsOnline`: the message names each degraded partition and the CPUs of
  its cores that are online too. It starts
  `partitions publish no devices while the kernel contradicts their smt`, or, with
  `taintDegradedPartitions` on, `partitions are degraded while the kernel contradicts their smt`.
  `dra_cpu_partition_verified` is `0` for each such partition.
- `False`, reason `PartitionsVerified`: the kernel has online the threads per core every
  partition's `smt` declares.
- `Unknown`, reason `DriverStopped`: the driver has stopped and checks nothing.

The driver decides this at start, and the condition holds until the driver restarts. Take the CPUs
the message names offline at boot, through the platform's configuration, then restart the driver
pod. What the driver does with CPUs taken offline while it runs is undefined.

Meanwhile the driver refuses to prepare a claim on a degraded partition's device, with one of:

```text
request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline); tolerate dra.cpu/degraded=SiblingThreadsOnline:NoSchedule, or take its sibling threads offline
request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline) and its devices are not published (`taintDegradedPartitions: false`); take its sibling threads offline
request "vcpu": partition "spdk" is degraded (SiblingThreadsOnline) and its devices are not published; take its sibling threads offline
```

- The first where the devices are published tainted and the claim's request does not tolerate the
  taint, which a scheduler without the `DRADeviceTaints` feature gate lets through: add the
  toleration to the request, or take the threads offline.
- The second where `taintDegradedPartitions` is off, for a claim allocated the device before the
  driver restarted: take the threads offline, then restart the driver pod.
- The third once the API server has dropped the taint, which an API server without
  `DRADeviceTaints` does: the driver then withholds the partition's devices, refuses every claim on
  them, tolerating or not, and counts it in `dra_cpu_device_taints_dropped_total`, until it
  restarts. Enable the gate on the API server and restart the driver pod, or take the threads
  offline.

The condition has a second cause, a `reserved` partition holding CPUs of a claim prepared before the
partitions changed. The driver finds such claims at start, from the claims it restores, and keeps
serving everything else. It kills the running containers of each such claim, refuses to create or
start them again, and keeps the condition `True` with reason `ReservedHoldsClaims`, or
`SiblingThreadsOnline` where a partition's `smt` is contradicted too, the message naming every
problem. The driver pod is not ready while such a claim is prepared. Delete the pods using the
claims the message names: once the last such claim is unprepared, the condition turns `False` with
reason `PartitionsVerified`, or stays `True` with `SiblingThreadsOnline` where a partition's `smt`
is still contradicted, and the driver pod is ready again.

## A Node Missing Its Kubelet CPU Reservation

On a node whose partitions keep CPUs from pods without claims, the kubelet must reserve those CPUs
through `kubeReserved` or `systemReserved`, or the scheduler and the kubelet admit pods without
claims against CPUs they never run on. The driver counts every online CPU outside the `default`
partition: every thread of the cores its `exclusive` and `reserved` partitions name, every CPU of a
named partition of role `default`, and every thread a partition's `smt` expects offline that is
online. It compares that count with what the kubelet reserves, the Node's CPU capacity minus its
allocatable CPU. Where the count is 0, on a node without `cpuPartitions` or whose partitions keep no
CPU from those pods, the driver checks nothing and keeps no condition for it.

Where the kubelet reserves less, the node is degraded: the driver publishes no devices, refuses to
prepare every claim, so pods holding claims stay `Pending` with a `FailedPrepareDynamicResources`
event naming the reservation, and its pod is not ready, so a rolling update stops at the first such
node. It still unprepares claims, and running containers keep their CPUs. The driver reports it in
the condition `CPUReservationMissing` on its Node:

```bash
kubectl get node <node> -o jsonpath='{.status.conditions[?(@.type=="CPUReservationMissing")]}'
```

- `True`, reason `InsufficientCPUReservation`: the message gives the reservation to set.
  `dra_cpu_node_reservation_ok` is `0`.
- `False`, reason `SufficientCPUReservation`: the message gives what the kubelet reserves and the
  count.
- `Unknown`, reason `DriverStopped`: the driver has stopped and checks nothing.

A node degrades when the driver's partitions are installed before the kubelet reserves their CPUs,
when its partitions grow without the reservation, when the kubelet's configuration loses it, or when
the boot configuration brings more of a partition's CPUs online. Drain the node, set the reservation
the message gives and restart the kubelet: the driver reads the Node again when the kubelet updates
it, publishes its devices and turns the condition `False`. Without `list` and `watch` on nodes the
driver sees the change only when it restarts.

Where the count includes threads a partition's `smt` expects offline, the message names them and
gives as the reservation to set the count without them, to be set together with taking them offline
at boot. Once they are offline the kubelet counts them in neither its capacity nor what it reserves,
so reserving them too would take as many CPUs from the pods without claims; where they are all the
default partition has, nothing would be left to them. Where the kubelet already reserves the count
without them and they are still online, the message only asks to take them offline at boot.

An alert on the condition needs a `for:` longer than a driver restart, during which the condition
is `Unknown`.

If the Node object is deleted and the kubelet registers it again within 40 seconds, the running
driver's ResourceSlices still name the old Node, and the garbage collector removes each one the
driver publishes. Delete the driver pod on that node.

## Integration With Bug Reports

1. Run `dracpu gatherinfo` on the affected node.
1. Attach the generated `.yaml` file.
1. Include relevant driver logs from the same node.

## Collecting From a Cluster

The tool is node-local by design. To collect data from every node in a cluster, run it once per driver pod and save each YAML stream using the pod's node name.

Example:

```bash
namespace=kube-system  # the namespace the driver was installed into
selector='app.kubernetes.io/name=dra-driver-cpu'
outdir=./dracpu-gatherinfo

mkdir -p "${outdir}"

for pod in $(kubectl get pods -n "${namespace}" -l "${selector}" -o name); do
  node=$(kubectl get -n "${namespace}" "${pod}" -o jsonpath='{.spec.nodeName}')
  kubectl exec -n "${namespace}" "${pod}" -- /dracpu gatherinfo --stdout > "${outdir}/${node}.yaml"
done
```

If you want to keep multiple node reports in a single stream, concatenate them as YAML documents:

```bash
for pod in $(kubectl get pods -n "${namespace}" -l "${selector}" -o name); do
  kubectl exec -n "${namespace}" "${pod}" -- /dracpu gatherinfo --stdout >> all-nodes.yaml
done
```

Each invocation starts with `---`, so the combined file remains a valid multi-document YAML stream.
