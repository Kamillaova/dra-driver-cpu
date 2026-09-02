# Fork notes

This is a fork of [kubernetes-sigs/dra-driver-cpu](https://github.com/kubernetes-sigs/dra-driver-cpu)
adding **mutable CPU placement** and **CCX-aware runtime defragmentation**: the driver may move a
running claim's CPUs between uncore caches (AMD CCX / L3) without restarting the container, so that
alignment lost to claim churn is recovered.

## Baseline

| Component         | Baseline                                                                                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Upstream commit   | `a869a56b05133265e5cefa094af62121c6607d04` (`v0.2.0-249-ga869a56`, "helm: reject unknown top-level values")                                 |
| Kubernetes API    | `k8s.io/*` v0.37.0                                                                                                                          |
| Go                | 1.26.0                                                                                                                                      |
| Container runtime | containerd >= v2.4.0-beta.0 (requires [containerd/nri#301](https://github.com/containerd/nri/pull/301), first vendored as nri v0.12.1)      |
| CRI-O             | unsupported: v1.36 still vendors nri v0.12.0, which holds the `Adaptation` lock across `updateFn` and can deadlock on an unsolicited update |

## Required cluster feature gates

- `DRAConsumableCapacity` — already required by upstream `grouped` mode; also gates the
  `requestPolicy` this fork publishes for `fullPhysicalCPUsOnly`.
- `DRANodeAllocatableResources` — optional, but recommended. Without it the scheduler's node-level
  `cpu` accounting is blind to DRA claims. See `docs/user/workload-requirements.md`.

## Migration between upstream and this fork

The `DRA_CPUSET_<claimUID>` variable keeps upstream's name and format, so neither direction needs a
reader for a legacy format and a claim's identity survives either swap.

**upstream → fork: no drain.** Identity comes from the variable's name. A spec written by upstream
carries no `dra.cpu/cpuset` annotation, so the fork falls back to parsing that spec's own env value,
which for an upstream spec is the real placement. Each claim gains an annotation the next time its spec
is written.

**fork → upstream: no drain while nothing moves a claim**, which holds until the defragmenter lands.
The annotation always agrees with the env value, so an upstream driver reading these specs behaves
exactly as it does with its own.

Once claims can move, **a rollback requires draining the node first.** An upstream driver treats the
env value as the claim's placement, and a moved claim's value no longer describes its container.
Depending on the value, upstream either rejects the claim — leaving the container holding none, which
it then classifies as shared and flattens onto the shared pool, losing both its exclusivity and its
CPUs — or skips the container entirely, leaving its CPUs to be handed out to others as well. Draining
avoids both. Runbook: drain the node, confirm no remaining pod holds a `dra.cpu` claim
(`--ignore-daemonsets` skips DaemonSet pods, and static pods cannot be evicted at all), then roll the
DaemonSet.

## Conventions

- **Minimal diff to upstream.** New behaviour lives in new packages (`pkg/defrag`, additions to
  `pkg/cpuinfo`). Touch points inside existing files are single thin calls rather than inline logic.
- **Every upstream declaration or block the fork changes carries a `// CCX-FORK:` comment** naming what
  upstream does instead, so `git grep 'CCX-FORK:'` enumerates every point a rebase has to reconcile. A
  symbol the fork *adds* needs no marker: there is nothing upstream to reconcile it against; the
  divergence surface below lists them.
- Commits follow upstream's conventions (`type:` / `component:` subjects, `Signed-off-by`) and are kept
  atomic so the upstreamable ones cherry-pick cleanly.

### Divergence surface

Fork-only symbols added to upstream files, which carry no marker of their own. Additions that belong
to an upstreamable piece (see below) are not repeated here: they leave with their PR.

- `pkg/driver/cdi.go`: `cdiCPUSetAnnotation`, `GetDeviceCPUSet`

- `pkg/store/cpu_allocation.go`: `BeginRebind`, `CommitRebind`, `AbortRebind`, `GetRebindOrigin`

- the packages' existing `_test.go` files: the fork's unit tests are added in place, beside the code
  they pin, rather than kept apart

Wholly new files (`pkg/defrag`, `pkg/coreselect`, `reconcile.go`, `pkg/cpuinfo/coretopology.go`) are
visible to `git diff --stat` on their own and are not repeated here.

`CdiManager.GetDeviceEnv` is upstream's and is kept, but the fork's driver no longer calls it now that
placement comes from the annotation. It stays on the `cdiManager` interface to keep the diff small.

## Upstreamable pieces

These carry no fork-only code and are intended to be offered upstream as separate PRs:

- physical-core identity helpers in `pkg/cpuinfo`
- uncore cache geometry device attributes
- claim-ownership authentication against `Container.CDIDevices` (as an *additive* check)
- `fullPhysicalCPUsOnly` — upstream issue #45
- shared-pool reconcile after unprepare — upstream issue #279

The defragmenter itself is not upstreamable in the near term: it needs a runtime opt-in, and moving a
running container's cpuset is a semantic upstream has not sanctioned.

## Deviations

Record here any place where reality contradicted the design (renamed upstream API, different observed
behaviour) rather than silently adapting.

- **`BeginRebind` validates only the claim's CPU count**, plus the overlap and state-machine invariants
  the store owns. Preserving the per-NUMA footprint and taking whole cores are properties of the
  target, enforced where the target is chosen; duplicating them in the store would give the same
  policy two homes.
