#!/bin/bash
#
# Renders the chart's DaemonSet for the kubelet roots worth checking and hands
# each one to the semantic validator in test/render. Helm and the case list live
# here so the Go side only has to judge an object it was given.

set -euo pipefail

chart=${CHART:-deployment/helm/dra-driver-cpu}
helm=${HELM:-helm}
default_root=/var/lib/kubelet

# With no cluster to ask, helm renders against its own built-in Kubernetes
# version, and in the helm the Makefile installs that version is below the
# chart's kubeVersion range, which refuses the chart outright. Named here so the
# render does not depend on which helm ran it, and named at the oldest stable
# release the range covers so these cases reach it.
kube_version=${HELM_KUBE_VERSION:-1.34.0}
template_args=(
	template test "${chart}"
	--kube-version "${kube_version}"
	--show-only templates/daemonset.yaml
)

tmp=$(mktemp -d)
trap 'rm -rf "${tmp}"' EXIT

# render <name> <root>; writes ${tmp}/<name>.yaml. It does not print where the
# file landed, because a function called in $(...) reports the status of its last
# command, so a succeeding echo would hide a failing helm and leave the test
# reading an empty file.
render() {
	local name=$1 root=$2
	local args=("${template_args[@]}")
	if [[ -n ${root} ]]; then
		args+=(--set-string "kubeletRootDir=${root}")
	fi
	"${helm}" "${args[@]}" >"${tmp}/${name}.yaml"
}

# accepts <name> <root> <expected-root> <expect-root-flag>
accepts() {
	local name=$1 root=$2 expected=$3 want_flag=$4
	echo "render check: ${name}"
	render "${name}" "${root}"
	go test -count=1 ./test/render/ -args \
		-manifest "${tmp}/${name}.yaml" \
		-expected-root "${expected}" \
		-expect-root-flag="${want_flag}"
}

# checkNodeName <name> <want_config> [helm args...]; the chart must supply the Node name
# through the NODE_NAME downward API env var and must not pass the removed
# --hostname-override flag. An explicit hostnameOverride reaches the driver
# through the config file and must fold into the rendered config.yaml.
checkNodeName() {
	local name=$1 want_config=$2
	shift 2

	echo "node name render check: ${name}"
	"${helm}" "${template_args[@]}" "$@" \
		>"${tmp}/${name}-daemonset.yaml"
	go test -count=1 ./test/render/ -args \
		-manifest "${tmp}/${name}-daemonset.yaml" \
		-expected-root "${default_root}" \
		-expect-root-flag=false \
		-check-hostname-override

	if [[ -n ${want_config} ]]; then
		"${helm}" template test "${chart}" --kube-version "${kube_version}" \
			--show-only templates/configmap.yaml "$@" \
			>"${tmp}/${name}-configmap.yaml"
		if ! grep -Fq "hostnameOverride: ${want_config}" "${tmp}/${name}-configmap.yaml"; then
			echo "the rendered config does not contain hostnameOverride: ${want_config}" >&2
			return 1
		fi
	fi
}

# checkUpdateStrategy <name> <expected-json> [helm args...]; the chart passes
# updateStrategy through as written. Every other case expects none, the default
# that leaves the API server's rolling update with maxSurge 0.
checkUpdateStrategy() {
	local name=$1 want=$2
	shift 2

	echo "update strategy render check: ${name}"
	"${helm}" "${template_args[@]}" "$@" >"${tmp}/${name}.yaml"
	go test -count=1 ./test/render/ -args \
		-manifest "${tmp}/${name}.yaml" \
		-expected-root "${default_root}" \
		-expect-root-flag=false \
		-expected-update-strategy "${want}"
}

# checkThreadedMode <name> <mode> [helm args...]; the host access the chart
# renders for driverConfig.cgroupThreadedMode. Every other case expects false,
# which renders none.
checkThreadedMode() {
	local name=$1 want=$2
	shift 2

	echo "threaded mode render check: ${name}"
	"${helm}" "${template_args[@]}" "$@" >"${tmp}/${name}.yaml"
	go test -count=1 ./test/render/ -args \
		-manifest "${tmp}/${name}.yaml" \
		-expected-root "${default_root}" \
		-expect-root-flag=false \
		-expected-threaded-mode "${want}"
}

# checkAdmissionPolicies <name> <expect-policies> <fullname> [helm args...]; the
# admission policies, their bindings and the verbs they confine render with
# rbac.create alone, matching the release's own service account.
checkAdmissionPolicies() {
	local name=$1 want=$2 fullname=$3
	shift 3

	echo "admission policies render check: ${name}"
	"${helm}" template test "${chart}" --kube-version "${kube_version}" \
		--namespace dra-test "$@" >"${tmp}/${name}-chart.yaml"
	go test -count=1 ./test/render/ -args \
		-chart "${tmp}/${name}-chart.yaml" \
		-expect-admission-policies="${want}" \
		-release-namespace dra-test \
		-fullname "${fullname}"
}

# refuses <name> <message> <helm args...>; the render has to fail and name the
# guard the case is for, so an unrelated helm failure cannot satisfy it.
refuses() {
	local name=$1 want=$2
	shift 2
	echo "render check: ${name} (expected to be refused)"
	if "${helm}" "${template_args[@]}" \
		"$@" >"${tmp}/${name}.yaml" 2>"${tmp}/${name}.err"; then
		echo "the chart accepted ${name}, want a refusal" >&2
		return 1
	fi
	if ! grep -Fq -- "${want}" "${tmp}/${name}.err"; then
		echo "${name} was refused for another reason, want ${want}:" >&2
		cat "${tmp}/${name}.err" >&2
		return 1
	fi
}

# checkDeviceClasses <name> <want> [helm args...]; the DeviceClasses the chart
# renders, comma-separated in name order, and the selector of each.
checkDeviceClasses() {
	local name=$1 want=$2
	shift 2

	echo "device classes render check: ${name}"
	"${helm}" template test "${chart}" --kube-version "${kube_version}" \
		--show-only templates/deviceclass.yaml "$@" >"${tmp}/${name}-classes.yaml"
	go test -count=1 ./test/render/ -args \
		-chart "${tmp}/${name}-classes.yaml" \
		-check-device-classes \
		-expected-device-classes "${want}"
}

# renders <name> <helm args...>; the render has to succeed.
renders() {
	local name=$1
	shift
	echo "render check: ${name}"
	"${helm}" "${template_args[@]}" "$@" >"${tmp}/${name}.yaml"
}

# values <name> <yaml>; writes a values file and prints its path.
values() {
	printf '%s\n' "$2" >"${tmp}/$1-values.yaml"
	echo "${tmp}/$1-values.yaml"
}

# Unset: the chart leaves the root to the driver, so there is no flag to check.
accepts default '' "${default_root}" false

# The driver serves liveness only at /livez and readiness only at /healthz, so
# the chart intentionally does not expose a probe path override.
refuses healthz-path healthzPath --set-string healthzPath=/readyz

accepts relocated /var/lib/custom-kubelet /var/lib/custom-kubelet true
# Cleaned by the template, so the mounts and the flag cannot differ by a slash.
accepts uncleaned /var/lib/custom-kubelet/ /var/lib/custom-kubelet true
# The root the chart cleans whole paths for: appending would give //plugins.
accepts root-dir / / true
# Cleaning removes the segment carrying the sequence, so nothing reaches the
# PodSpec to be reinterpreted and the value is the default after all.
accepts cleaned-away '/var/lib/$(IGNORED)/../kubelet' "${default_root}" false
# A lone $ is left alone by the kubelet, so the guard below refuses the two
# sequences rather than the character, and this is what says so.
accepts single-dollar '/var/lib/$kubelet' '/var/lib/$kubelet' true
# 73 bytes, the most the root can spend and still leave the socket at 107.
accepts socket-budget "/$(printf 'x%.0s' {1..72})" "/$(printf 'x%.0s' {1..72})" true

# The node name cases, judged by their own dedicated checks.
checkNodeName fallback ''
checkNodeName args-override mynode \
	--set-string 'args.hostnameOverride=mynode'
checkNodeName config-override mynode \
	--set-string 'driverConfig.hostnameOverride=mynode'

checkUpdateStrategy on-delete '{"type":"OnDelete"}' \
	--set 'updateStrategy.type=OnDelete'
checkUpdateStrategy rolling-update '{"type":"RollingUpdate","rollingUpdate":{"maxUnavailable":"10%"}}' \
	--set 'updateStrategy.type=RollingUpdate' \
	--set-string 'updateStrategy.rollingUpdate.maxUnavailable=10%'

checkThreadedMode threaded-false false --set 'driverConfig.cgroupThreadedMode=false'
checkThreadedMode threaded-create create --set-string 'driverConfig.cgroupThreadedMode=create'
checkThreadedMode threaded-place place --set-string 'driverConfig.cgroupThreadedMode=place'

checkAdmissionPolicies admission-policies true test-dra-driver-cpu
checkAdmissionPolicies admission-policies-fullname true cpu \
	--set-string 'fullnameOverride=cpu'
checkAdmissionPolicies admission-policies-no-rbac false test-dra-driver-cpu \
	--set 'rbac.create=false'

# The guard this change adds.
refuses dollar-paren 'must not contain' \
	--skip-schema-validation --set-string 'kubeletRootDir=/var/lib/$(KUBELET_ROOT)'
refuses double-dollar 'must not contain' \
	--skip-schema-validation --set-string 'kubeletRootDir=/var/lib/$$kubelet'

# A name ending in a space is a different directory, so trimming would move the
# mounts and the flag together to somewhere the kubelet is not.
refuses trailing-space 'must not begin or end with whitespace' \
	--skip-schema-validation --set-string 'kubeletRootDir=/var/lib/custom-kubelet '

# A key helm looks up by exact name, so a miscased one renders the default while
# the operator believes the kubelet was relocated.
refuses miscased-key 'differs from kubeletRootDir only in case' \
	--skip-schema-validation --set-string 'kubeletrootdir=/var/lib/custom-kubelet'

# One byte past what the case above spends, so the pair pins the limit rather
# than only showing that something far over it is refused.
refuses over-budget 'over the 107-byte limit' \
	--skip-schema-validation --set-string "kubeletRootDir=/$(printf 'x%.0s' {1..73})"

# A cpuPartitions list switches a node into partitions only with the entry
# "default", and the driver refuses a list naming partitions without it; the
# chart refuses it at render with the driver's message.
no_default='cpuPartitions names partitions but no partition "default": add `- name: default` to cpuPartitions to use partitions'
refuses partitions-reserved-without-default "${no_default}" -f "$(values partitions-reserved-without-default '
driverConfig:
  cpuPartitions:
  - name: system
    role: reserved
    cpuset: "0-1"')"
refuses partitions-vms-without-default "${no_default}" -f "$(values partitions-vms-without-default '
driverConfig:
  cpuPartitions:
  - name: vms
    role: exclusive
    cpuset: "2-3"')"
renders partitions-default-entry -f "$(values partitions-default-entry '
driverConfig:
  cpuPartitions:
  - name: default')"
# An entry's thread count left null is the key left out.
renders partitions-null-thread-count -f "$(values partitions-null-thread-count '
driverConfig:
  cpuPartitions:
  - name: default
    smt: null')"
# The list left out, null and empty all mean no partitions.
renders partitions-empty -f "$(values partitions-empty '
driverConfig:
  cpuPartitions: []')"
renders partitions-null -f "$(values partitions-null '
driverConfig:
  cpuPartitions: null')"
renders partitions-left-out -f "$(values partitions-left-out '
driverConfig:
  groupBy: socket')"
# The external allocator and machine-wide grouping, which needs it, belong to
# the mode without partitions.
refuses partitions-allocator-external 'invalid allocator "external" with cpuPartitions: must be "cpumanager"' \
	-f "$(values partitions-allocator-external '
driverConfig:
  allocator: external
  cpuPartitions:
  - name: default')"
refuses partitions-groupby-machine 'invalid groupBy "machine" with cpuPartitions: needs allocator "external", which partitions do not use' \
	-f "$(values partitions-groupby-machine '
driverConfig:
  groupBy: machine
  allocator: external
  cpuPartitions:
  - name: default')"

# Every profile's list holds the entry too; the chart names the profile.
refuses profile-vms-without-default "profile \"r7625\": ${no_default}" -f "$(values profile-vms-without-default '
driverConfig:
  profiles:
    r7625:
      cpuPartitions:
      - name: vms
        role: exclusive
        cpuset: "2-3"')"
# Within profiles a list left out, null or empty is refused: every node runs
# a declared profile, and a profile without the entry describes no node.
no_partitions='profile "r7625" has no partitions; add `- name: default` to its cpuPartitions'
refuses profile-list-left-out "${no_partitions}" -f "$(values profile-list-left-out '
driverConfig:
  profiles:
    r7625: {}')"
refuses profile-list-null "${no_partitions}" -f "$(values profile-list-null '
driverConfig:
  profiles:
    r7625:
      cpuPartitions: null')"
refuses profile-list-empty "${no_partitions}" -f "$(values profile-list-empty '
driverConfig:
  profiles:
    r7625:
      cpuPartitions: []')"
renders profile-default-entry -f "$(values profile-default-entry '
driverConfig:
  profiles:
    r7625:
      cpuPartitions:
      - name: default')"
# A node whose cores are all alike runs a declared profile, which may be named
# default.
renders profile-named-default -f "$(values profile-named-default '
driverConfig:
  profiles:
    default:
      cpuPartitions:
      - name: default')"
# A partition name is one device class on every node.
refuses profile-two-roles 'partition "vms" has role "exclusive" in profile "r7625" and "reserved" in profile "x3d"; a partition keeps one role in every profile' \
	-f "$(values profile-two-roles '
driverConfig:
  profiles:
    r7625:
      cpuPartitions:
      - name: default
      - name: vms
        role: exclusive
        cpuset: "2-3"
    x3d:
      cpuPartitions:
      - name: default
      - name: vms
        role: reserved
        cpuset: "0-1"')"
renders profile-one-role -f "$(values profile-one-role '
driverConfig:
  profiles:
    r7625:
      cpuPartitions:
      - name: default
      - name: vms
        role: exclusive
        cpuset: "2-3"
    x3d:
      cpuPartitions:
      - name: default
      - name: vms
        role: exclusive
        cpuset: "4-7"')"
refuses profile-allocator-external 'invalid allocator "external" with profiles: must be "cpumanager"' \
	-f "$(values profile-allocator-external '
driverConfig:
  allocator: external
  profiles:
    r7625:
      cpuPartitions:
      - name: default')"
refuses profile-groupby-machine 'invalid groupBy "machine" with profiles: needs allocator "external", which partitions do not use' \
	-f "$(values profile-groupby-machine '
driverConfig:
  groupBy: machine
  allocator: external
  profiles:
    r7625:
      cpuPartitions:
      - name: default')"

# One DeviceClass per partition that can publish devices: dra.cpu for the
# entry "default", dra.cpu.<name> for every other partition but a reserved one,
# across the fleet-wide list and every profile, each name once.
checkDeviceClasses classes-without-partitions 'dra.cpu'
checkDeviceClasses classes-off '' --set deviceClass.create=false
checkDeviceClasses classes-partitions 'dra.cpu,dra.cpu.pool,dra.cpu.vms' -f "$(values classes-partitions '
driverConfig:
  cpuPartitions:
  - name: default
  - name: system
    role: reserved
    cpuset: "0-1"
  - name: vms
    role: exclusive
    cpuset: "4-7"
  - name: pool
    role: default
    cpuset: "2-3"')"
checkDeviceClasses classes-profiles 'dra.cpu,dra.cpu.infra,dra.cpu.spdk,dra.cpu.vms' -f "$(values classes-profiles '
driverConfig:
  profiles:
    r7625:
      cpuPartitions:
      - name: default
      - name: vms
        role: exclusive
        cpuset: "16-127"
      - name: spdk
        role: exclusive
        cpuset: "8-15"
    x3d:
      cpuPartitions:
      - name: default
      - name: system
        role: reserved
        cpuset: "0-1"
      - name: vms
        role: exclusive
        cpuset: "8-31"
      - name: infra
        role: default
        cpuset: "2-7"')"

echo "render checks passed"
