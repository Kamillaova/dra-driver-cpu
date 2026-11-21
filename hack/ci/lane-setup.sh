#!/usr/bin/env bash
# Copyright The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Prepares the host for an e2e lane (pre, before the kind cluster is created)
# and the cluster for it (post, after). pre prints the lane's variables as
# NAME=value lines for the Makefile's ci-kind-setup, and leaves out each
# variable whose shape the host lacks, so the specs needing it skip.

set -euo pipefail

# The CPUs kind's control plane and every pod without a claim run on.
readonly MIN_DEFAULT_CPUS=2

fail() {
	echo "lane-setup: $*" >&2
	exit 1
}

as_root() {
	if [ "$(id -u)" -eq 0 ]; then
		"$@"
	else
		sudo -n "$@"
	fi
}

expand_cpus() {
	local part
	local -a parts
	IFS=, read -ra parts <<<"$1"
	for part in "${parts[@]}"; do
		if [[ ${part} == *-* ]]; then
			seq "${part%-*}" "${part#*-}"
		else
			echo "${part}"
		fi
	done
}

count_cpus() {
	if [ -z "$1" ]; then
		echo 0
		return
	fi
	expand_cpus "$1" | wc -l | tr -d ' '
}

overlaps() {
	[ -n "$1" ] && [ -n "$2" ] && [ -n "$(comm -12 <(expand_cpus "$1" | sort) <(expand_cpus "$2" | sort))" ]
}

core_of() {
	local topology=/sys/devices/system/cpu/cpu$1/topology
	if [ -r "${topology}/core_cpus_list" ]; then
		cat "${topology}/core_cpus_list"
	else
		cat "${topology}/thread_siblings_list"
	fi
}

# The CPUs of every core a CPU of $1 lies on.
cores_of() {
	[ -n "$1" ] || return 0
	local cpu
	for cpu in $(expand_cpus "$1"); do
		expand_cpus "$(core_of "${cpu}")"
	done | sort -nu | paste -sd, -
}

# The threaded specs move pinned threads into threaded children of a claim's
# group, which needs cpuset in a threaded subtree: fail here rather than let
# them skip on a kernel without it.
probe_threaded_cpuset() {
	local root=/sys/fs/cgroup
	[ "$(stat -fc %T "${root}")" = cgroup2fs ] || fail "${root} is not a cgroup2 mount"
	grep -qw cpuset "${root}/cgroup.subtree_control" || fail "cpuset is not enabled in ${root}/cgroup.subtree_control"
	local probe=${root}/dracpu-e2e-probe-$$
	as_root mkdir "${probe}" "${probe}/child"
	local ok=true
	echo threaded | as_root tee "${probe}/child/cgroup.type" >/dev/null || ok=false
	if ${ok}; then
		echo +cpuset | as_root tee "${probe}/cgroup.subtree_control" >/dev/null || ok=false
	fi
	as_root rmdir "${probe}/child" "${probe}"
	${ok} || fail "cannot enable cpuset in a threaded cgroup subtree on kernel $(uname -r)"
}

partitions_pre() {
	echo "lane-setup: kernel $(uname -r)" >&2
	probe_threaded_cpuset

	local -a online cores
	local -A seen
	local cpu core
	mapfile -t online < <(expand_cpus "$(cat /sys/devices/system/cpu/online)")
	for cpu in "${online[@]}"; do
		core=$(core_of "${cpu}")
		if [ -z "${seen[${core}]:-}" ]; then
			seen[${core}]=1
			cores+=("${core}")
		fi
	done

	local nosmt=${DRACPU_E2E_NOSMT_PARTITION_CPUS:-}
	local degraded=${DRACPU_E2E_DEGRADED_PARTITION_CPUS:-}
	local reserved=${DRACPU_E2E_RESERVED_PARTITION_CPUS:-}
	local left=$((${#online[@]} - $(count_cpus "${nosmt}") - $(count_cpus "$(cores_of "${degraded}")") - $(count_cpus "${reserved}")))
	local i
	local -a threads
	if [ -z "${nosmt}" ]; then
		for ((i = ${#cores[@]} - 1; i >= 0; i--)); do
			core=${cores[i]}
			if [ "$(count_cpus "${core}")" -eq 1 ] && ! overlaps "${core}" "${reserved}" &&
				[ $((left - 1)) -ge "${MIN_DEFAULT_CPUS}" ]; then
				nosmt=${core}
				left=$((left - 1))
				break
			fi
		done
	fi
	# The degraded partition lists one thread of a core whose other threads are
	# online, and takes the whole core out of default. It comes before reserved,
	# so a host of two cores of two threads keeps one core in default.
	if [ -z "${degraded}" ]; then
		for ((i = ${#cores[@]} - 1; i >= 0; i--)); do
			core=${cores[i]}
			if [ "$(count_cpus "${core}")" -gt 1 ] && ! overlaps "${core}" "${reserved}" &&
				[ $((left - $(count_cpus "${core}"))) -ge "${MIN_DEFAULT_CPUS}" ]; then
				mapfile -t threads < <(expand_cpus "${core}")
				degraded=${threads[0]}
				left=$((left - $(count_cpus "${core}")))
				break
			fi
		done
	fi
	if [ -z "${reserved}" ]; then
		for ((i = ${#cores[@]} - 1; i >= 0; i--)); do
			core=${cores[i]}
			if ! overlaps "${core}" "${nosmt}" && ! overlaps "${core}" "${degraded}" &&
				[ $((left - $(count_cpus "${core}"))) -ge "${MIN_DEFAULT_CPUS}" ]; then
				reserved=${core}
				left=$((left - $(count_cpus "${core}")))
				break
			fi
		done
	fi

	echo "lane-setup: nosmt partition '${nosmt}', degraded partition '${degraded}', reserved partition '${reserved}', ${left} CPUs left to default" >&2
	[ -z "${nosmt}" ] || echo "DRACPU_E2E_NOSMT_PARTITION_CPUS=${nosmt}"
	[ -z "${degraded}" ] || echo "DRACPU_E2E_DEGRADED_PARTITION_CPUS=${degraded}"
	[ -z "${reserved}" ] || echo "DRACPU_E2E_RESERVED_PARTITION_CPUS=${reserved}"
}

# crun 1.30.1's build with systemd support, and the SHA-256 its release's
# CHECKSUMS gives for each.
readonly CRUN_VERSION=1.30.1
declare -rA CRUN_SHA256=(
	[amd64]=86d1e6a0e76945975d3aebfab39cbc6a26eea15f1c3fc66b6776d19e5dc346a0
	[arm64]=0ef30b0fbb494dc9c5303d46b091a84e27cb8ac99c985123fb1eb339854b69ea
)
readonly CRUN_HANDLER=crun-writable
# crun's systemd driver talks to systemd through the system bus, which kind's
# node image does not run; Debian's dbus at the version its release, trixie,
# carries.
readonly DBUS_VERSION=1.16.2-2

crun_pre() {
	partitions_pre
	# The specs show crun's annotations under place, where the driver moves
	# the container's process.
	echo "DRACPU_E2E_THREADED_MODE=place"
}

# crun_post installs crun in every node of the cluster, and under systemd the
# system bus it needs, checks that each kubelet runs the lane's cgroup driver,
# and creates the RuntimeClass of the crun handler. Under systemd it also runs
# the driver's own pod under crun.
crun_post() {
	local driver=$1
	local cluster=${CLUSTER_NAME:?CLUSTER_NAME names the kind cluster}
	local arch
	case "$(uname -m)" in
	x86_64) arch=amd64 ;;
	aarch64 | arm64) arch=arm64 ;;
	*) fail "no crun build for $(uname -m)" ;;
	esac
	local dir
	dir=$(mktemp -d)
	curl -fsSL -o "${dir}/crun" "https://github.com/containers/crun/releases/download/${CRUN_VERSION}/crun-${CRUN_VERSION}-linux-${arch}"
	echo "${CRUN_SHA256[${arch}]}  ${dir}/crun" | sha256sum -c - >&2 || fail "crun ${CRUN_VERSION} for ${arch} does not match its checksum"
	chmod 0755 "${dir}/crun"
	local node
	for node in $(kind get nodes --name "${cluster}"); do
		if [ "${driver}" = systemd ]; then
			docker exec "${node}" sh -c "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends dbus=${DBUS_VERSION} && systemctl start dbus" >&2 ||
				fail "cannot run the system bus in node ${node}"
		fi
		docker cp "${dir}/crun" "${node}:/usr/local/bin/crun"
		docker exec "${node}" /usr/local/bin/crun --version >&2
		grep -qx "cgroupDriver: ${driver}" <(docker exec "${node}" cat /var/lib/kubelet/config.yaml) ||
			fail "the kubelet of node ${node} does not run the ${driver} cgroup driver"
	done
	rm -rf "${dir}"
	kubectl apply -f - <<EOF2
apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: ${CRUN_HANDLER}
handler: ${CRUN_HANDLER}
EOF2
	if [ "${driver}" = systemd ]; then
		# The chart has no value for it.
		kubectl -n kube-system patch daemonset dracpu --type merge \
			-p "{\"spec\":{\"template\":{\"spec\":{\"runtimeClassName\":\"${CRUN_HANDLER}\"}}}}"
		kubectl -n kube-system rollout status daemonset dracpu --timeout=180s
	fi
}

# Upstream's last release, chart 0.3.1 by its digest, with the values its own
# e2e install passes (Makefile at v0.3.1) and the chart's own image.
readonly UPSTREAM_CHART=oci://registry.k8s.io/dra-driver-cpu/charts/dra-driver-cpu@sha256:4bd9830401fb50df1e7be11574a64532469ce4788712bf1583127aae1f94027d
readonly -a UPSTREAM_VALUES=(
	--namespace kube-system
	--set fullnameOverride=dracpu
	--set podLabels.app=dracpu
	--set args.logLevel=6
	--set args.cpuDeviceMode=grouped
	--set args.groupBy=numanode
	--set-string args.reservedCPUs=0
	--set args.exposePCIeRoots=true
	--set driverConfig.publishNodeAllocatableResourceMapping=false
	--set driverConfig.allocator=cpumanager
)

upgrade_post() {
	"${HELM:-helm}" install dra-driver-cpu "${UPSTREAM_CHART}" "${UPSTREAM_VALUES[@]}" --wait --timeout 5m
	"$(dirname "$0")/wait-resourcelices.sh"
}

[ $# -eq 2 ] || fail "usage: $0 <lane> pre|post"
lane=$1
step=$2
case "${lane}/${step}" in
partitions/pre | gates-off/pre) partitions_pre ;;
partitions/post | gates-off/post | upgrade/pre) ;;
upgrade/post) upgrade_post ;;
crun-systemd/pre | crun-cgroupfs/pre) crun_pre ;;
crun-systemd/post) crun_post systemd ;;
crun-cgroupfs/post) crun_post cgroupfs ;;
*) fail "unknown lane or step: ${lane} ${step}" ;;
esac
