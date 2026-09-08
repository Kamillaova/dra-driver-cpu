# The Capacity Mirror

> [!NOTE]
> This page describes a **fork-only** mechanism. See [Fork notes](../../FORK.md).

Under [`groupBy: uncorecache`](configuration.md#driver-configuration) a device is one uncore cache, and
[defragmentation](defragmentation.md) moves a running claim between caches. A claim's allocation cannot
be rewritten, so a claim that has moved is still charged to the cache its allocation names. The driver
therefore publishes each cache's capacity with that difference folded in, and a scheduler subtracting
what its own records charged arrives at the CPUs really free there.

The practical consequence, and the reason this page exists: **a cache device's published `capacity` is
not its size** while any claim on the node sits off the cache it was allocated from. The size is
published beside it as `dra.cpu/numCPUs`, which never changes.

## The arithmetic

For each device, the driver publishes

```
value = size + departed − squatters
```

| term | meaning |
| --- | --- |
| `size` | the device's own allocatable CPUs, which is what it would publish with nothing moved |
| `departed` | CPUs charged to this device by claims that have left it |
| `squatters` | CPUs occupied on this device by claims charged elsewhere |

Both terms are sums over the claims this driver has prepared. A claim allocated but not yet prepared
contributes nothing: its charge and its placement are the same device until something moves it, and
counting it earlier would inflate the value for the seconds between the allocation being written and
the claim being prepared.

A worked example on 16-CPU caches, with an 8-CPU claim consolidated from cache B to cache A while A
already holds 8 CPUs of other work:

| | before | after the move | after that claim exits |
| --- | --- | --- | --- |
| A: occupied / charged / published | 8 / 8 / 16 | 16 / 8 / **8** | 8 / 8 / 16 |
| B: occupied / charged / published | 8 / 8 / 16 | 0 / 8 / **24** | 0 / 0 / 16 |
| a whole-cache request on B | refused, 8 free | **admitted**: 24 − 8 = 16 | admitted |
| a 2-CPU request on A | admitted, 8 free | refused: 8 − 8 = 0 | admitted |

B publishes 24 and is charged 8, so 16 CPUs are on offer there and the cache really is empty. A
publishes 8 and is charged 8, so nothing more fits and the cache really is full. When the moved claim
ends, B's charge drops to 0 and the driver publishes 16 again.

## What a move does, in order

1. The round reserves the move. The claim now holds both the CPUs it is taking and the ones it is
   leaving, so the destination is short of them at once and no departure is credited to the origin.
   Both halves are pessimistic, which is the only safe direction while nobody can say which of the two
   the container is on.
2. The driver publishes, and then waits until its node's own `ResourceSlice`s show those devices as it
   means them — publication is asynchronous and nothing reports when a write landed. A shrink that is
   not stored in time abandons the round: the claim stays where it is, the reservation is released, and
   the scope is retried. While the API server cannot store a slice, nothing on the node moves.
3. The containers are updated, in one batch.
4. Once the runtime confirms, the origin grows.

An exchange of two claims' CPUs is the same construction and shrinks both caches, crediting neither a
departure until the runtime confirms.

The acknowledgement in step 2 compares the whole device, not its capacity alone, and requires the pool
to be whole. The publishing controller adopts fields the API server dropped and carries on, so a device
whose number is right and whose taint is gone would read as stored while over-advertising exactly the
CPUs the taint was there to withdraw; and a consumer does not allocate from an incomplete pool at all,
so a capacity spread over several slices says nothing until the last slice is stored.

## The capacity floor

A shareable device's request policy is re-validated against a changed value, and a value below the
minimum plus the step invalidates the whole `ResourceSlice`. A rejected slice leaves **every** device of
that node stale while the publishing controller retries it, so a computed value below the floor is
published at the floor instead, and the device is tainted:

| taint | key | tolerated by |
| --- | --- | --- |
| partition access | `dra.cpu/partition` | claim templates naming that partition, with `operator: Equal` |
| capacity floor | `dra.cpu/floor` | nothing |
| poisoning | `dra.cpu/poisoned` | nothing |

The three keys are distinct on purpose: a toleration written to reach a partition must not also
tolerate a device whose capacity is over-stated, or a fenced one. The floor taint carries no value, and
goes when the computed value climbs back. `dra_cpu_capacity_mirror_floored_devices` counts the devices
in that state, and is worth an alert: while it is above zero, capacity is being withheld from the fleet.

With whole-core allocation the floor is four CPUs — a minimum of one core and a step of one core — so on
16-CPU caches a device reaches it only when thirteen or more of its CPUs are held by claims charged
elsewhere.

## Windows this does not close

Two remain, both one hop wide, and both end at Prepare rather than in a running workload:

- **A scheduler acting on a capacity it has not observed yet.** The driver's own wait covers the write
  to the API server, not the scheduler's read of it.
- **An allocation cleared before the claim's CPUs are free.** The resourceclaim controller may clear a
  claim's allocation before the kubelet unprepares it, so for that hop the origin credits a departure
  the allocator has already refunded.

Either way a claim can arrive at a device with no room. Prepare refuses it, the pod waits bound, and
the kubelet retries; `dra_cpu_prepare_no_room_total` counts it, labelled by whether the allocator could
have split the claim at all.

## Operating notes

- **Reading a slice.** `capacity` is the mirrored number; `dra.cpu/numCPUs` is the physical size. A
  device whose two disagree has claims charged to it that are not on it, or claims on it that are
  charged elsewhere.
- **An omitted capacity request.** A device's request policy keeps its default at the physical size,
  which validation compares only against the minimum and maximum. A request that names no amount
  therefore asks for the whole physical cache and is refused by a device publishing less. Name the
  amount.
- **Rollback.** Disabling defragmentation stops new moves; it does not restore the capacities of
  claims that have already moved, which stay corrected until those claims end. A driver rolled back
  past this mechanism publishes plain sizes again while claims sit off their recorded caches, which
  over-advertises those caches — drain the node first, as
  [Fork notes](../../FORK.md) already requires for a rollback past mutable placement.
- **Upgrade.** A claim whose record on disk predates this mechanism has no charged devices recorded, and
  is left out of the arithmetic rather than guessed at. The kubelet replays Prepare for every claim when
  the driver restarts, and that is where the answer comes back, so the correction is right one restart
  after the upgrade.

## Upstream

`DeviceCapacity.Value` is documented upstream as "the fixed total capacity", which "does not change".
Mechanically a driver may change it — slices are republished, a changed capacity is re-validated rather
than frozen, and the allocator recomputes `value − consumed` from the live slice on every allocation —
but the mechanism is outside the model the field describes, and is recorded here as such. It exists
because `status.allocation` is immutable once populated: the recorded device cannot move with the claim,
so the published capacity is the only instrument left. A per-device driver-held unavailable amount, or
an allocation reassignment API, would replace it.
