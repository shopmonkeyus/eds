package util

import (
	"fmt"
	"os"
	"strings"
)

// IsRunningInsideDocker returns true if the process is running inside a docker container.
func IsRunningInsideDocker() bool {
	fmt.Println("IsRunningInsideDocker 1")
	if Exists("/.dockerenv") {
		return true
	}
	fmt.Println("IsRunningInsideDocker 2")

	if Exists("/proc/1/cgroup") {
		buf, _ := os.ReadFile("/proc/1/cgroup")
		if len(buf) > 0 {
			contents := strings.TrimSpace(string(buf))
			return strings.Contains(contents, "docker") || strings.Contains(contents, "lxc") || strings.Contains(contents, "rt")
		}
	}
	fmt.Println("IsRunningInsideDocker 3")

	return false
}
