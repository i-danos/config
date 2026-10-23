// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

//go:build linux

package schema

import (
	"syscall"
	"unsafe"
)

const clockMonotonic = 1 // CLOCK_MONOTONIC

// MonotonicNowUs returns CLOCK_MONOTONIC in microseconds: the clock systemd
// uses for ActiveEnterTimestampMonotonic. Dispatch records and the send
// window are expressed in it. It does not advance while the system is
// suspended.
func MonotonicNowUs() int64 {
	var ts syscall.Timespec
	_, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME,
		clockMonotonic, uintptr(unsafe.Pointer(&ts)), 0)
	if errno != 0 {
		return 0
	}
	return ts.Sec*1000000 + ts.Nsec/1000
}
