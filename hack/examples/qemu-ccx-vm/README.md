# QEMU VMs on CCX-aligned CPU claims

Two VM classes against this fork's driver, plus the in-pod shim that keeps QEMU
correct while the defragmenter moves claims under it.

| File                           | Shows                                                                                                     |
| ------------------------------ | --------------------------------------------------------------------------------------------------------- |
| `vm-whole-ccx.yaml`            | A VM owning one whole uncore cache: capacity sized to the cache, CEL selector on `largestUncoreCacheCPUs` |
| `vm-small-flexible.yaml`       | The movable class: two whole cores, no selector, re-homed by the defragmenter when it helps a larger VM   |
| `qemu-launcher-configmap.yaml` | The workload contract in code: live CPUs from the cgroup, per-vCPU pinning via QMP, inotify re-pinning    |

The image is yours to build; the shim needs `qemu-system-x86_64`, `python3` and
`inotify-tools`. `/dev/kvm` is granted via `privileged` here for brevity — use a
device plugin in production.

## The contract, in one paragraph

Never parse `DRA_CPUSET_*`: with `defragEnabled` its value is the literal string
`dynamic`, and without it it is only a snapshot from container creation. Read
the live set from `cpuset.cpus.effective` (or `sched_getaffinity(2)`), pin vCPU
k to the k-th CPU of the cache-ordered set, and watch `cpuset.cpus` with inotify — the
writable file generates events on every move, the derived `.effective` does
not, so watch one and read the other. A 30-second poll backstops the two
changes inotify cannot see: CPU hotplug and a parent cgroup shrinking. The CPU
*count* never changes across a move, so `-smp` chosen at boot stays valid.

## vCPU threads vs control threads

QEMU is more than its vCPUs: the main loop, iothreads, vhost workers and RCU
threads all need CPU time, and left unpinned beside the vCPUs they steal it
from them.

**With a shared partition on the node (recommended):** the claim carries a
second request for a share of that pool, and the launcher reads the CPUs it was
given from that request's own device metadata file, mounted for these templates at
`/var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/vcpus/helpers/dra.cpu-metadata.json`
(`vcpus` being the name the pod gives the claim; a claim referenced by name instead sits under
`resourceclaims/<claim>/`).
Every thread that is not a vCPU goes there, alongside whatever else shares the
pool. A pool never moves, so unlike an exclusive placement those CPUs are safe
to read once and keep.

**Without one:** control threads cannot leave the claim -- a container's threads
cannot be pinned outside its own cgroup cpuset -- so drop the helpers request,
set `CONTROL_CPUS` and size the vCPU request *plus one core*: the launcher
carves the cache-ordered tail, a whole core, out of the claim itself.

It must be one claim with two requests either way, not a vcpus claim plus a
helpers claim: a container holding two claims is pinned to their union, and
nothing inside the container can tell which CPUs belong to which -- both
identity variables say `dynamic`.

## Sizing rules worth knowing

- With `fullPhysicalCPUsOnly` the device publishes a `requestPolicy` whose step
  is the core size; the scheduler rounds an odd request up, visibly in the
  claim's `status.allocation.devices.results[].consumedCapacity`.
- A claim sized to exactly one cache stays cache-aligned even when moved: the
  only place it fits is another whole cache.
- The whole-CCX selector keeps the pod off nodes that could never satisfy it
  aligned; whether a cache is *currently* free is the defragmenter's job, not
  the scheduler's.

See [CPU Defragmentation](../../../docs/user/defragmentation.md) for the
feature itself and [Workload Configuration
Requirements](../../../docs/user/workload-requirements.md) for how to set the
standard resources with and without KEP-5517 accounting.
