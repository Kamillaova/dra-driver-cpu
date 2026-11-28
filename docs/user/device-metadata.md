# Device Metadata Files

A container that uses a `dra.cpu` claim gets a read-only JSON file for each request of the claim,
describing the devices that request was given. A workload reads its own allocation there, for
example the CPUs to pin its threads to, without being told out of band.

The file is a Kubernetes feature, [KEP-5304](https://github.com/kubernetes/enhancements/issues/5304).
The DRA plugin library writes it, with a CDI spec that mounts it read-only into the container; the
driver decides which attributes it carries.

## Where it is

For a claim that the pod names in `pod.spec.resourceClaims[].resourceClaimName`:

```text
/var/run/kubernetes.io/dra-device-attributes/resourceclaims/{claimName}/{requestName}/dra.cpu-metadata.json
```

For a claim generated from a ResourceClaimTemplate, whose name is generated, the directory is named
after `pod.spec.resourceClaims[].name` instead:

```text
/var/run/kubernetes.io/dra-device-attributes/resourceclaimtemplates/{podClaimName}/{requestName}/dra.cpu-metadata.json
```

`{requestName}` is the request's name. When the request lists alternatives in `firstAvailable`, the
directory keeps the request's name, and the file names the chosen alternative in
`requests[0].name`, as `<request>/<subrequest>`. The file name carries the driver's name, so two
drivers that serve one request do not collide.

## What is in it

The file is a JSON stream: the same content once for each metadata API version the driver
configures, in the driver's order, one object per line. This driver configures two versions,
`metadata.resource.k8s.io/v1beta1` first and `metadata.resource.k8s.io/v1alpha1` second. A reader
takes the first object whose `apiVersion` it understands, so it keeps working when a version is
added or removed.

The first object for a request of 4 CPUs on a NUMA node device, with some of the device's published
attributes left out:

```json
{
  "kind": "DeviceMetadata",
  "apiVersion": "metadata.resource.k8s.io/v1beta1",
  "metadata": { "name": "dataplane-cpus", "namespace": "net", "uid": "…", "generation": 1 },
  "requests": [
    {
      "name": "poll",
      "devices": [
        {
          "driver": "dra.cpu",
          "pool": "worker-1",
          "name": "cpudevnuma000",
          "attributes": {
            "resource.kubernetes.io/numaNode": { "int": 0 },
            "dra.cpu/socketID": { "int": 0 },
            "dra.cpu/numCPUs": { "int": 32 },
            "dra.cpu/smtEnabled": { "bool": true },
            "dra.cpu/threadsPerCore": { "int": 2 },
            "dra.cpu/allocatedNumCPUs": { "int": 4 },
            "dra.cpu/cpuRequest": { "string": "4" },
            "dra.cpu/cpuLimit": { "string": "max" },
            "dra.cpu/cpuset": { "string": "2-3,34-35" }
          }
        }
      ]
    }
  ]
}
```

Only the request that the file belongs to appears in `requests`. The other requests of the claim have
their own files.

## Attributes

Each device carries the attributes it publishes in its `ResourceSlice`, described in
[Device Attributes and Selectors](device-attributes.md), and these, which exist only in the file:

| Attribute                  | Type   | Description                                                                                                                                                                                                                                                   |
| -------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dra.cpu/cpuset`           | string | The CPUs the request holds on this device, in cpuset list form, for a request that holds exclusive CPUs, in either device mode                                                                                                                                |
| `dra.cpu/cpuRequest`       | string | The CPU amount booked for the request on this device, as a quantity such as `4` or `1500m`, when the claim consumes `dra.cpu/cpu` capacity                                                                                                                    |
| `dra.cpu/cpuLimit`         | string | The request's CPU limit as a quantity, or `max` where the request has none; a shared request's is the `cpuConfig.limit` it writes, else its amount, `max` where it books nothing, and a request that holds exclusive CPUs has none; written with `cpuRequest` |
| `dra.cpu/allocatedNumCPUs` | int    | **Deprecated:** use `dra.cpu/cpuRequest`. The number of CPUs allocated from the device, written for each request that holds exclusive CPUs when the claim consumes `dra.cpu/cpu` capacity                                                                     |

`dra.cpu/cpuRequest` and `dra.cpu/cpuLimit` mirror a container's CPU request and limit.

A shared request, one on a device of a partition of role `default` such as the `dra.cpu` class on a
node with `cpuPartitions`, holds no CPU alone, and its CPUs can move while its container keeps the file
it started with. So its file carries neither `dra.cpu/cpuset` nor `dra.cpu/allocatedNumCPUs`: a workload
reads its current CPUs from its own CPU affinity, for example `Cpus_allowed_list` in
`/proc/self/status`. Its `dra.cpu/cpuRequest` is the amount it booked, `0` where it books nothing, and
its `dra.cpu/cpuLimit` is the cap the claim asks for: the `cpuConfig.limit` the claim writes for it,
such as `1` beside an amount of `250m`, `max` for `limit: max`, and else the same amount, `max` where
it books nothing. A CPU limit on the container replaces that cap, see
[Workload Configuration Requirements](workload-requirements.md#capped-shared-requests). Like
every file it carries its device's attributes, so its `dra.cpu/numCPUs` is the device's CPU count, not
the share's. A request that holds exclusive CPUs keeps `dra.cpu/allocatedNumCPUs` with upstream's
meaning.

A request that spans several devices has one entry per device in `devices`, and each entry's
`dra.cpu/cpuset` names the CPUs the request holds on that device alone. The CPUs of the whole request
are the union of the entries. A container that uses several requests, or several claims, runs on the
CPUs of all of them; `DRA_EXCLUSIVE_ASSIGNED_CPUSET` holds the union of their exclusive CPUs, see
[Workload Configuration Requirements](workload-requirements.md).

## Reading it

```bash
f=/var/run/kubernetes.io/dra-device-attributes/resourceclaims/dataplane-cpus/poll/dra.cpu-metadata.json
# The first object of a version this reader understands, then the CPUs of each device.
jq -c 'select(.apiVersion == "metadata.resource.k8s.io/v1beta1")' "$f" | head -n 1 |
  jq -r '.requests[0].devices[].attributes["dra.cpu/cpuset"].string // empty'
```

## When the file is missing

If the plugin library cannot write the file, it logs `Failed to write device metadata` in the
driver's log and still reports the claim prepared, so the pod starts without the file. A workload
that reads the file fails then. Recreate the pod to get the file.
