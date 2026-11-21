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

partitions_pre() {
	echo "lane-setup: kernel $(uname -r)" >&2

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

[ $# -eq 2 ] || fail "usage: $0 <lane> pre|post"
lane=$1
step=$2
case "${lane}/${step}" in
partitions/pre) partitions_pre ;;
partitions/post) ;;
*) fail "unknown lane or step: ${lane} ${step}" ;;
esac
