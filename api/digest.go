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

package api

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
)

const DeviceAttributeMaxValueLength = 64

func FrontierInputDigest[T ~string](claimUIDs []T) string {
	sorted := make([]string, len(claimUIDs))
	for i, uid := range claimUIDs {
		sorted[i] = string(uid)
	}
	slices.Sort(sorted)
	h := sha256.New()
	fmt.Fprint(h, strings.Join(sorted, "\n"))
	return fmt.Sprintf("%x", h.Sum(nil))
}
