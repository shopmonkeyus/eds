package util

import (
	"fmt"
	"os"
	"runtime"

	"github.com/denisbrodbeck/machineid"
)

// SystemInfo returns the operating system details
type SystemInfo struct {
	ID           string `json:"system_id"`
	Hostname     string `json:"hostname"`
	NumCPU       int64  `json:"num_cpu"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	GoVersion    string `json:"go_version"`
}

// GetSystemInfo returns info about the system
func GetSystemInfo() (*SystemInfo, error) {
	var s SystemInfo
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("error getting hostname: %w", err)
	}
	id, err := machineid.ProtectedID("pinpoint-agent")
	if err != nil {
		return nil, fmt.Errorf("error getting system id: %w", err)
	}
	s.ID = id
	s.Hostname = hostname
	s.OS = runtime.GOOS
	s.NumCPU = int64(runtime.NumCPU())
	s.Architecture = runtime.GOARCH
	s.GoVersion = runtime.Version()[2:]
	return &s, nil
}
