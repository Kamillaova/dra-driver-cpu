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
from them. Where they belong depends on one driver setting.

**With `sharedPoolCPUs` configured (recommended):** the driver appends the pool
CPUs of the claim's NUMA nodes to the container's cpuset and names them in
`DRA_SHARED_CPUS`. The launcher pins vCPUs onto `sched_getaffinity() minus DRA_SHARED_CPUS` -- the exclusive CPUs -- and leaves every other thread to the
kernel scheduler inside the pool, alongside whatever else shares it. The claim
is sized to the vCPUs alone. `DRA_SHARED_CPUS` is the one placement fact that is
safe to read from the environment: the pool is static, and a move preserves the
claim's NUMA footprint, so the value cannot go stale.

**Without a pool:** control threads cannot leave the claim -- a container's
threads cannot be pinned outside its own cgroup cpuset -- so set `CONTROL_CPUS`
and size the claim vCPUs *plus one core*: the launcher carves the cache-ordered
tail, a whole core, out of the claim itself.

It must be one claim either way, not a vcpus claim plus a control claim: a
container holding two claims is pinned to their union, and nothing inside the
container can tell which CPUs belong to which -- both identity variables say
`dynamic`. A per-NUMA shared *device* with oversubscribed capacity and admission
control remains future work; the static pool is its
carve-out half, built.

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
