# CCX-Aligned Scheduling

> [!IMPORTANT]
> This is a feature of [this fork](../../FORK.md), not of upstream `dra-driver-cpu`.

[Defragmentation](defragmentation.md) recovers cache alignment *within* a node, and structurally
cannot fix the choice of node. What a scheduler sees of a grouped device is one number, the capacity
nothing has consumed yet, and a number has no shape: 32 free CPUs scattered two per uncore cache and
32 forming two whole caches are the same 32. A claim wanting a whole cache is bound to whichever node
the scheduler liked, and a bound claim never moves to another node.

`publishFitAnnotation` publishes the shape that number leaves out, so a scheduler can prefer the node
that can actually align the claim.

```yaml
# values.yaml
driverConfig:
  cpuDeviceMode: grouped
  groupBy: numanode
  publishFitAnnotation: true
```

The chart grants `patch` on `nodes` only while this is set. Nothing else about the driver changes:
publishing writes an annotation and asks the runtime for nothing, so unlike defragmentation it needs
no `assumeUnsolicitedUpdatesSafe`, and it works with the defragmenter off.

## The annotation

`dra.cpu/fit`, on the driver's own Node. On a two-socket EPYC 9554 with one reserved core and one
shared-pool core, both in NUMA node 0's first cache, and one 8-CPU claim straddling that node's
first two caches:

```json
{
  "v": 1,
  "policy": "spread",
  "numaNodes": [
    {"id": 0, "cacheCPUs": [12, 16, 16, 16], "freeCPUs": [8, 12, 16, 16], "repackedFreeCPUs": [12, 8, 16, 16]},
    {"id": 1, "cacheCPUs": [16, 16, 16, 16], "freeCPUs": [16, 16, 16, 14], "repackedFreeCPUs": [16, 16, 16, 14]}
  ]
}
```

Three arrays per NUMA node, index-aligned, one entry per uncore cache:

| field              | meaning                                                                                                                                         |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `cacheCPUs`        | what the cache can hold: net of the reserved set and any static shared pool, and rounded down to whole cores where `fullPhysicalCPUsOnly` is on |
| `freeCPUs`         | what no claim holds right now                                                                                                                   |
| `repackedFreeCPUs` | what would be free once the defragmenter has moved what it is **actually willing** to move                                                      |
| `policy`           | the node's `cachePlacementPolicy`, so a reader simulating a placement follows the one deployed                                                  |

The ragged `12` is real: cache 0 lost a reserved core and a pool core. That is why `cacheCPUs` is
published rather than assumed uniform — a reader deciding whether a cache is untouched compares
`freeCPUs` against it, and a reader assuming every cache is the size of the largest would conclude
that cache 0 can never be clean.

A reader may rely on `0 <= freeCPUs[i] <= cacheCPUs[i]`, the same for `repackedFreeCPUs`, and
`sum(freeCPUs) <= sum(repackedFreeCPUs)`. It should treat a payload violating any of them, or
carrying a `v` it does not know, as if the node published nothing.

### Why the repacked shape is the settled one

`repackedFreeCPUs` is where repeated passes actually converge, not the ideal packing. The planner
permanently honours its minimum gain and the shared-pool guard, so the ideal can be forever out of
reach — and a scheduler steering a claim to a node for a consolidation that never arrives is worse
off than one told nothing at all.

For the same reason the repacked shape is simply today's whenever nothing is going to act on it:

- **`defragEnabled: false`.** Nobody is going to repack the node.
- **A claim holding half a physical core**, prepared before `fullPhysicalCPUsOnly` was turned on. The
  odd thread a move would release cannot back a whole-core claim, so counting it free would
  contradict `cacheCPUs` in the same payload.

## What it costs

One annotation on one Node object, and only when its value changes. A shape that has not moved costs
neither a read nor a write. Changes are debounced for a couple of seconds, because a pod's claims are
prepared one at a time and only the shape they leave behind is worth publishing.

On top of that the driver re-reads its own Node about once a minute, jittered, and rewrites the
annotation if it does not match — a write is not the only way an annotation can change.

Two things it deliberately will not do:

- **It publishes nothing until the NRI `Synchronize` hook has rebuilt its stores.** Before that the
  allocation store is empty because nothing has told the driver otherwise, not because the node is
  idle, and an annotation written then would advertise a busy node as entirely free. After a driver
  restart the previous annotation is still accurate — placements survive the restart — so the gap
  costs nothing.
- **A driver configured *not* to publish removes a stale annotation at startup.** Nothing else would:
  it describes a node whose driver has stopped describing it. Removing it needs the same `patch` the
  chart grants only while the feature is on, so if you disable the feature and drop the RBAC in one
  step, the driver will log the fallback: `kubectl annotate node <node> dra.cpu/fit-`.

## The scheduler side

The reader is `CCXAlign`, a score-only plugin on a branch of
[kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins), run as a
second scheduler that pods opt into with `schedulerName`. It scores each node by whether the pod's
claims land on the fewest caches their size allows — now, after a repack, or not at all — and prefers
placing a small claim on a node whose caches are already dirty, keeping clean caches for the claims
that need them.

Score-only is deliberate. Fragmentation is mutable state, and a stale filter would make pods
unschedulable in flaps; a stale score only costs a slightly worse choice. Staleness here is
asymmetric in the safe direction: claims the scheduler itself added are accounted for in its own
reservations, and both claim releases and defragmentation only ever improve a node's shape.

Nodes that publish nothing score as unknown rather than as bad, so a fleet part-way through a driver
rollout degrades to "no information" instead of blacklisting every node that has not upgraded.

## Limits

- The signal is advisory. It improves the distribution of outcomes and guarantees nothing for any
  single pod; when every node is equally fragmented it has nothing to say.
- It does not replace defragmentation. It reduces how often the defragmenter is handed a node that
  cannot be repaired.
- Under `groupBy: socket` the annotation is still true, but it describes NUMA nodes while the devices
  are sockets, and the plugin maps the two by the device's `dra.cpu/numaNodeID` attribute. Use
  `groupBy: numanode` for it to be read.
