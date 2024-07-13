package util

import (
	"fmt"
	"net"
	"runtime"

	"github.com/denisbrodbeck/machineid"
)

// SystemInfo returns the operating system details
type SystemInfo struct {
	NumCPU       int64  `json:"num_cpu"`
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	GoVersion    string `json:"go_version"`
}

// GetSystemInfo returns info about the system
func GetSystemInfo() (*SystemInfo, error) {
	var s SystemInfo
	s.OS = runtime.GOOS
	s.NumCPU = int64(runtime.NumCPU())
	s.Architecture = runtime.GOARCH
	s.GoVersion = runtime.Version()[2:]
	return &s, nil
}

// GetMachineId returns a unique machine ID
func GetMachineId() (string, error) {
	return machineid.ProtectedID("eds")
}

// GetLocalIP returns the local private IP address
func GetLocalIP() (string, error) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addresses {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.IsPrivate() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.To4().String(), nil
			}
		}
	}
	return "", fmt.Errorf("no private IP found")
}
