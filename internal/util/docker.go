package util

import (
	"os"
	"strings"
)

// IsRunningInsideDocker returns true if the process is running inside a docker container.
func IsRunningInsideDocker() bool {
	if Exists("/.dockerenv") {
		return true
	}
	if Exists("/proc/1/cgroup") {
		buf, _ := os.ReadFile("/proc/1/cgroup")
		if len(buf) > 0 {
			contents := strings.TrimSpace(string(buf))
			return strings.Contains(contents, "docker") || strings.Contains(contents, "lxc") || strings.Contains(contents, "rt")
		}
	}
	return true
}
