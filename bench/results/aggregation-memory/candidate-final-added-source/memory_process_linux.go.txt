//go:build linux

package db

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processResidentBytes() (int64, error) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	v := strings.Fields(string(data))
	if len(v) < 2 {
		return 0, fmt.Errorf("aptevadb: malformed process memory counters")
	}
	n, err := strconv.ParseInt(v[1], 10, 64)
	return n * int64(os.Getpagesize()), err
}
