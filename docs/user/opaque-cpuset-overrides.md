# Custom Opaque CPUSet Allocation Overrides

> [!NOTE]
> **Audience for the `cpuset` field: scheduler-plugin authors and platform integrators**, not
> workload authors. It documents an integration contract for external schedulers; no in-tree
> scheduler implements it today. Regular workloads should use the default `numanode`/`socket`
> grouping instead. The `relocatable` and `alignment` fields below are the workload author's,
> and apply under every grouping.

When using `grouped` device mode with the `groupBy: machine` configuration, the DRA driver does not perform automatic topology-aware CPU allocation. Instead, an explicit core assignment must be provided via the `cpuset` field in the claim's opaque configuration parameters.

The Kubelet driver parses this configuration at prepare time from the claim's allocation status (`status.allocation.devices.config`). The control plane (typically scheduling plugin) is responsible for injecting this configuration block into the allocation result when binding the claim.

## Opaque Parameters Schema (`v1alpha1`)

The `opaque.parameters` field must conform to the following schema:

| Field                   | Type   | Description                                                                                                          |
| :---------------------- | :----- | :------------------------------------------------------------------------------------------------------------------- |
| `apiVersion`            | string | Must be set to `v1alpha1`.                                                                                           |
| `cpuConfig`             | object | Container object for CPU configurations.                                                                             |
| `cpuConfig.cpuset`      | string | Specifies the list of CPU cores in standard Linux cpuset format (e.g. `"2-5"`, `"0,4-6"`).                           |
| `cpuConfig.relocatable` | bool   | Permits the driver to change which CPUs back the claim while its containers run. Defaults to `false`.                |
| `cpuConfig.alignment`   | string | `BestEffort` or `Repairable`: what the claim asks about being placed across more caches than it needs. Defaults to `BestEffort`. |

### What the claim states about its own placement

`relocatable` is the tenant's answer to a question only the workload can answer: whether it survives
having its CPUs changed underneath it. A launcher that re-reads its affinity when `cpuset.cpus`
changes loses nothing; one that pinned its threads once at start keeps running with that pinning
silently gone. The default is therefore `false`, and a workload that follows the
[contract](workload-requirements.md) states `true` explicitly. Only a `relocatable: true` claim is
ever moved by [defragmentation](defragmentation.md).

`alignment` says what to do about landing on more uncore caches than the claim's size requires:

- `BestEffort` runs where the allocator placed it and stays there. This is the plain exclusive
  request, and the default.
- `Repairable` runs split and has the driver make the claim whole, which means moving other claims
  out of the way, so it requires `relocatable: true`.

Both are read back to the container in its request's [device metadata file](device-metadata.md), so a
workload sees the contract it is running under.

Neither says anything about a claim the allocator has no choice about. A claim whose requests offer
no alternatives cannot be split whatever it asks for, so setting `alignment` there is refused rather
than ignored.

## Example of a Fully Allocated ResourceClaim with Opaque Configuration

```yaml
apiVersion: resource.k8s.io/v1
kind: ResourceClaim
metadata:
  name: exclusive-cores-claim
  namespace: default
  uid: claim-uid-123456
spec:
  devices:
    requests:
    - name: cpu-request-1
      exactly:
        deviceClassName: dra.cpu
        count: 4
status:
  allocation:
    devices:
      results:
      - request: cpu-request-1
        driver: dra.cpu
        pool: test-node
        device: cpudevmachine
        consumedCapacity:
          dra.cpu/cpu: "4"
      config:  # Added by external scheduler
      - source: FromClaim
        requests:
        - cpu-request-1
        opaque:
          driver: dra.cpu
          parameters:
            apiVersion: v1alpha1
            cpuConfig:
              cpuset: "2-5"
```

The driver allocates the specified cores directly (after validating that they are allocatable and not reserved). If it is omitted, resource preparation will fail and pod startup will be rejected.

> [!IMPORTANT]
> **Validation Rules:**
>
> - The `cpuset` value must be specified in standard Linux cpuset format representing ranges and/or individual cores (e.g. `"2-5"`, `"1-10,15"`).
> - **Per-Request Configurations Only**: In `status.allocation.devices.config[]`, every configuration block's `requests` array must map to exactly one request in the claim. We do not support multiple claim requests mapping to the same cpuset config.
> - **ResourceClaim Source**: Opaque configurations must originate from the `ResourceClaim`. Configurations defined in DeviceClass (`spec.config[]`) are not supported and will result in a validation error. Since a configuration in the `DeviceClass` applies to all claims referencing it, configuring a `cpuset` there would assign the same CPUs to multiple claims, failing allocation due to conflict between claims.
> - **CPUSet Validation**: The driver verifies that the custom cpuset is valid for the host machine and is currently allocatable. It checks that:
>   - The cores are part of the node's online CPUs.
>   - The cores are not reserved using the driver's `--reserved-cpus` configuration flag.
> - **Contradictory placement fields**: `cpuConfig.cpuset` cannot be combined with
>   `cpuConfig.relocatable: true` — the named cores are what such a claim is for, and permitting the
>   driver to leave them contradicts naming them. `cpuConfig.alignment: Repairable` requires
>   `cpuConfig.relocatable: true`, because making a split claim whole moves its own CPUs.
>   `cpuConfig.alignment` may only be set on a claim whose requests offer the allocator alternatives.
>   `cpuConfig.relocatable` and `cpuConfig.alignment` describe the claim rather than one of its
>   requests, so two of a claim's configurations disagreeing about them is an error too.
> - **Unknown versions and values** are refused: an `apiVersion` this driver does not implement, or an
>   `alignment` outside the two values above, fails preparation rather than being ignored.
> - **Error Handling**: If validation fails (e.g. core conflict, size mismatch, duplicate target, or offline cores), the driver returns a failure immediately in Kubelet's `PrepareResourceClaims` hook, causing pod startup to fail. The Kubelet records that as a `FailedPrepareDynamicResources` event on the pod.
