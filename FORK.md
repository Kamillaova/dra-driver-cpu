# Fork notes

This is a fork of [kubernetes-sigs/dra-driver-cpu](https://github.com/kubernetes-sigs/dra-driver-cpu)
adding **mutable CPU placement** and **CCX-aware runtime defragmentation**: the driver may move a
running claim's CPUs between uncore caches (AMD CCX / L3) without restarting the container, so that
alignment lost to claim churn is recovered.

## Baseline

| Component         | Baseline                                                                                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Upstream commit   | `8db2cd8698f6f0b9b7b777520d375acfb82e297b` (`v0.2.0-243-g8db2cd8`)                                                                          |
| Kubernetes API    | `k8s.io/*` v0.37.0                                                                                                                          |
| Go                | 1.26.0                                                                                                                                      |
| Container runtime | containerd >= v2.4.0-beta.0 (requires [containerd/nri#301](https://github.com/containerd/nri/pull/301), first vendored as nri v0.12.1)      |
| CRI-O             | unsupported: v1.36 still vendors nri v0.12.0, which holds the `Adaptation` lock across `updateFn` and can deadlock on an unsolicited update |

## Required cluster feature gates

- `DRAConsumableCapacity` — already required by upstream `grouped` mode; also gates the
  `requestPolicy` this fork publishes for `fullPhysicalCPUsOnly`.
- `DRANodeAllocatableResources` — optional, but recommended. Without it the scheduler's node-level
  `cpu` accounting is blind to DRA claims. See `docs/user/workload-requirements.md`.

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

- the packages' existing `_test.go` files: the fork's unit tests are added in place, beside the code
  they pin, rather than kept apart

Wholly new files (`pkg/coreselect`, `reconcile.go`, `pkg/cpuinfo/coretopology.go`) are visible to
`git diff --stat` on their own and are not repeated here.

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

- _none yet_
