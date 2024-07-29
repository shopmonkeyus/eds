package util

import (
	"fmt"
	"net"
	"runtime"

	"github.com/denisbrodbeck/machineid"
	"github.com/shirou/gopsutil/v4/host"
)

// SystemInfo returns the operating system details
type SystemInfo struct {
	Host      *host.InfoStat `json:"host"`
	NumCPU    int64          `json:"num_cpu"`
	GoVersion string         `json:"go_version"`
}

// GetSystemInfo returns info about the system
func GetSystemInfo() (*SystemInfo, error) {
	var s SystemInfo
	var err error
	s.Host, err = host.Info()
	if err != nil {
		return nil, err
	}
	s.NumCPU = int64(runtime.NumCPU())
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
