# Metrics

The driver exposes Prometheus metrics on the existing HTTP `/metrics` endpoint served by `--bind-address` (default `:8080`).

> [!NOTE]
> Driver custom metrics are ALPHA. They are useful for early observability, but metric names, labels, buckets, and semantics may change in future releases.

Custom driver metrics can also be listed programmatically without starting the driver:

```bash
dracpu introspect metrics
```

The command prints JSON metadata for custom `dra_cpu_*` metrics only. It does not include default Go runtime, process, or Prometheus client metrics.

| Metric                                     | Type      | Labels   | Description                                                                                                                        |
| ------------------------------------------ | --------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `dra_cpu_allocated_cpus`                   | Gauge     | none     | CPUs currently allocated to prepared resource claims.                                                                              |
| `dra_cpu_available_cpus`                   | Gauge     | none     | CPUs still available for allocation after reserved and active claim CPUs are excluded.                                             |
| `dra_cpu_reserved_cpus`                    | Gauge     | none     | CPUs excluded from DRA management by driver configuration.                                                                         |
| `dra_cpu_resource_claims_active`           | Gauge     | none     | Resource claims currently recorded as active by the allocation store.                                                              |
| `dra_cpu_prepare_claims_total`             | Counter   | `result` | Per-claim `PrepareResourceClaims` results. `result` is `success`, `error`, or `unknown`.                                           |
| `dra_cpu_unprepare_claims_total`           | Counter   | `result` | Per-claim `UnprepareResourceClaims` results. `result` is `success`, `error`, or `unknown`.                                         |
| `dra_cpu_prepare_claim_duration_seconds`   | Histogram | none     | Per-claim prepare latency in seconds.                                                                                              |
| `dra_cpu_claim_allocated_cpus`             | Histogram | none     | CPUs allocated for each newly successful claim allocation.                                                                         |
| `dra_cpu_synchronize_skipped_claims_total` | Counter   | none     | Claims or containers `Synchronize` could not adopt from the runtime's reported state, skipped rather than aborting the whole call. |

## Defragmentation

Reported only when [`defragEnabled`](configuration.md#driver-configuration) is on. A node with one
uncore cache per NUMA node has no spread to recover, so its excess is permanently zero.

| Metric                                       | Type      | Labels      | Description                                                                                                                        |
| -------------------------------------------- | --------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `dra_cpu_defrag_excess_uncore_caches`        | Gauge     | none        | Uncore caches the node's claims span beyond the fewest their sizes allow. Zero means every claim is as well placed as it can be.   |
| `dra_cpu_defrag_largest_alignable_free_cpus` | Gauge     | `numa_node` | Most CPUs still free within a single uncore cache of that node: the largest claim it can take unsplit.                             |
| `dra_cpu_defrag_passes_total`                | Counter   | `result`    | Defragmentation passes. `error` covers a pass that reverted a move the runtime refused, or could not confirm what the runtime did. |
| `dra_cpu_defrag_moves_total`                 | Counter   | `result`    | Claim moves attempted. `error` is a move the runtime refused and the driver reverted.                                              |
| `dra_cpu_defrag_blocked_moves_total`         | Counter   | none        | Moves a better placement called for that a pass could not make, usually because another claim is in the way.                       |
| `dra_cpu_defrag_pass_duration_seconds`       | Histogram | none        | Defragmentation pass latency in seconds.                                                                                           |

`dra_cpu_defrag_largest_alignable_free_cpus` is the leading indicator: it says whether the *next*
large claim will land aligned, where the excess count says whether the last ones did. A steady
`dra_cpu_defrag_blocked_moves_total` with a non-zero excess means the node cannot reach a better
placement on its own — the claims in the way are themselves pinned, or there is no slack to move
them through.

The custom metrics intentionally avoid labels for namespace, pod, claim, device, node, socket, group
mode, and error reason. Those labels would either be high-cardinality or need more API design before
becoming part of the driver's metric surface. Node identity should come from scrape target labels.
`numa_node` is the one exception, on the one metric where a node-wide total would hide what matters:
claims are allocated per NUMA node, so free space in the wrong node is no help, and the label's
cardinality is fixed by the hardware.
