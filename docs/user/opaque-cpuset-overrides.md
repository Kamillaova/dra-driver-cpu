# Custom Opaque CPUSet Allocation Overrides

## Migrate from individual mode

To keep scheduler-controlled selection of exact CPUs that the deprecated `Individual` mode enabled,
use grouped mode with the external allocator.
Note that also NUMA-node or Socket grouping supports external allocator like the Machine grouping.
All grouping modes publish the external-allocator attributes, but the NUMA-node or Socket grouping
also retain a useful topology boundary:

```yaml
driverConfig:
  cpuDeviceMode: grouped
  groupBy: numanode # or socket
  allocator: external
```

The external allocator reads the selected grouped device's
[external-allocator attributes](device-attributes.md#external-allocator-attributes), selects
CPUs from that group, and writes the cpuset in the opaque configuration described below. The
driver validates and enforces the selection. `groupBy: machine` is also supported, but is less
preferred because it removes that topology boundary; use it only when the allocator must select
from the entire node.

> [!NOTE]
> **Audience: scheduler-plugin authors and platform integrators**, not workload authors. This
> page documents an integration contract for external schedulers; no in-tree scheduler
> implements it today. Regular workloads should use the default `numanode`/`socket` grouping
> instead.

When using the external allocator with grouped mode (`numanode`, `socket`, `uncorecache`, or
`machine`), the DRA driver does not perform automatic topology-aware CPU allocation. Instead, an
explicit core assignment must be provided via the `cpuset` field in the claim's opaque
configuration parameters. With `numanode`, `socket` or `uncorecache`, the cpuset must belong to
the group selected for the request.

The Kubelet driver parses this configuration at prepare time from the claim's allocation status (`status.allocation.devices.config`). The control plane (typically scheduling plugin) is responsible for injecting this configuration block into the allocation result when binding the claim.

## Opaque Parameters Schema (`v1alpha1`)

The `opaque.parameters` field must conform to the following schema:

| Field                 | Type             | Description                                                                                                                                                                                                                                                                                                    |
| :-------------------- | :--------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `apiVersion`          | string           | Must be set to `v1alpha1`.                                                                                                                                                                                                                                                                                     |
| `cpuConfig`           | object           | Container object for CPU configurations.                                                                                                                                                                                                                                                                       |
| `cpuConfig.cpuset`    | string           | Specifies the list of CPU cores in standard Linux cpuset format (e.g. `"2-5"`, `"0,4-6"`).                                                                                                                                                                                                                     |
| `cpuConfig.limit`     | number or string | The CPU limit of a shared request: a quantity above zero, such as `500m` or `2`, or `max` for none. See [Capped Shared Requests](workload-requirements.md#capped-shared-requests).                                                                                                                             |
| `cpuConfig.exclusive` | bool             | `true` takes CPUs of the request's own out of a shared device whose partition sets `allowExclusive`. Left out, a request is a share on a node with `cpuPartitions` and exclusive on one without. See [Exclusive requests on a default partition](cpu-partitions.md#exclusive-requests-on-a-default-partition). |

A `cpuset` on a shared request, one on the `dra.cpu` class of a node with `cpuPartitions`, is
undefined behavior.

`exclusive: false` asks for a share, and where the request's devices are owned there is none to
give, so the claim is refused when it is prepared: on a node without `cpuPartitions`, whose
`dra.cpu` devices are all owned, with

```text
request "cpus": `cpuConfig.exclusive: false`, but node "worker-a" runs without partitions, where dra.cpu gives exclusive CPUs only; add `- name: default` to cpuPartitions, or drop cpuConfig.exclusive
```

and on an `exclusive` partition's class, owned on every node, with

```text
request "vcpu": `cpuConfig.exclusive: false`, but class dra.cpu.vms gives exclusive CPUs only; drop cpuConfig.exclusive, or use a shared class such as dra.cpu
```

`exclusive: true` there changes nothing: those CPUs are exclusive already. Two config entries that
give one request `exclusive`, with the same value or not, and an `exclusive` from a DeviceClass are
refused as a `limit` is.

A `limit` is checked when the claim is prepared, and the claim is refused where:

- it is zero, `0`, `0m` or `"0"`, unlike a container's CPU limit, where `0` means no limit: `max` is
  the one way to write none; a negative or empty limit is refused the same way;
- two config entries give it to one request, or to one alternative of a `firstAvailable` request, with
  the same value or not;
- the shared request has no `dra.cpu/cpu` amount, `0` being enough, or the limit is below it;
- the request holds exclusive CPUs and the limit is not `max`, since a cap would throttle them;
- it comes from a DeviceClass.

Each alternative of a `firstAvailable` request is judged as a request, whichever one the scheduler
picks.

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
        capacity:
          requests:
            dra.cpu/cpu: "4"
status:
  allocation:
    devices:
      results:
      - request: cpu-request-1
        driver: dra.cpu
        pool: test-node
        device: cpudevnuma000
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
> - **Strict Keys**: Keys are case-sensitive. A key the schema above does not name, a key in another casing, or a key written twice fails the preparation.
> - The `cpuset` value must be specified in standard Linux cpuset format representing ranges and/or individual cores (e.g. `"2-5"`, `"1-10,15"`).
> - **Per-Request Configurations Only**: In `status.allocation.devices.config[]`, every configuration block's `requests` array must map to exactly one request in the claim. We do not support multiple claim requests mapping to the same cpuset config.
> - **ResourceClaim Source**: Opaque configurations must originate from the `ResourceClaim`. Configurations defined in DeviceClass (`spec.config[]`) are not supported and will result in a validation error. Since a configuration in the `DeviceClass` applies to all claims referencing it, configuring a `cpuset` there would assign the same CPUs to multiple claims, failing allocation due to conflict between claims.
> - **CPUSet Validation**: The driver verifies that the custom cpuset is valid for the host machine and is currently allocatable. It checks that:
>   - The cores are part of the node's online CPUs.
>   - The cores are not reserved using the driver's `reservedCPUs` configuration field.
> - **Error Handling**: If validation fails (e.g. core conflict, size mismatch, duplicate target, or offline cores), the driver returns a failure immediately in Kubelet's `PrepareResourceClaims` hook, causing pod startup to fail.
