/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cgroupfs

const (
	ownedWeight   = 1
	largestWeight = 10000
)

// Weights returns the cpu.weight of each threaded child of one thread root.
// shared holds, per child, the amounts in millicores of its shared requests,
// none for a child holding only owned CPUs. A child holding only owned CPUs
// weighs 1. The others weigh in proportion to the shares the kubelet gives a
// pod requesting the sum of their amounts, the largest 10000 and each at
// least 1, so the children split the container's weight and never change it.
func Weights(shared [][]int64) []uint64 {
	shares := make([]uint64, len(shared))
	var largest uint64
	for i, amounts := range shared {
		if len(amounts) == 0 {
			continue
		}
		var sum int64
		for _, a := range amounts {
			sum += a
		}
		shares[i] = milliCPUToShares(sum)
		largest = max(largest, shares[i])
	}
	weights := make([]uint64, len(shared))
	for i := range shared {
		if shares[i] == 0 {
			weights[i] = ownedWeight
			continue
		}
		weights[i] = max(shares[i]*largestWeight/largest, 1)
	}
	return weights
}

// milliCPUToShares is the kubelet's MilliCPUToShares (kubernetes v1.37.0
// pkg/kubelet/cm/helpers_linux.go:144-160), which gives an amount of 0 the
// kernel's minimum of 2.
func milliCPUToShares(milliCPU int64) uint64 {
	const (
		minShares     = 2
		maxShares     = 262144
		sharesPerCPU  = 1024
		milliCPUToCPU = 1000
	)
	shares := milliCPU * sharesPerCPU / milliCPUToCPU
	if shares < minShares {
		return minShares
	}
	if shares > maxShares {
		return maxShares
	}
	return uint64(shares)
}
