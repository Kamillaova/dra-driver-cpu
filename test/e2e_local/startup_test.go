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

package e2e_local

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("[Local] dracpu beside a driver holding its port", func() {
	ginkgo.It("should exit on the HTTP bind before it touches the kubelet root", func() {
		held, err := net.Listen("tcp", "127.0.0.1:0")
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		defer func() { _ = held.Close() }()
		kubeletRoot := ginkgo.GinkgoT().TempDir()

		cmdline := []string{binPath, "--bind-address=" + held.Addr().String(), "--kubelet-root-dir=" + kubeletRoot}
		fmt.Fprintf(ginkgo.GinkgoWriter, "running: %v\n", cmdline)

		// #nosec G204 -- the command and arguments are fixed above.
		out, err := exec.Command(cmdline[0], cmdline[1:]...).CombinedOutput()
		fmt.Fprintf(ginkgo.GinkgoWriter, "output:\n%s\n", out)
		var exitErr *exec.ExitError
		gomega.Expect(errors.As(err, &exitErr)).To(gomega.BeTrue(), "want a non-zero exit, got %v", err)
		gomega.Expect(string(out)).To(gomega.ContainSubstring("failed to bind HTTP server to " + held.Addr().String()))

		// The driver creates its plugin directory and sockets under the root, and
		// connects to NRI, only after the bind.
		entries, err := os.ReadDir(kubeletRoot)
		gomega.Expect(err).ToNot(gomega.HaveOccurred())
		gomega.Expect(entries).To(gomega.BeEmpty())
	})
})
