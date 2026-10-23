// Copyright (c) 2026, AT&T Intellectual Property. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//

//go:build !linux

package schema

import "time"

var processStart = time.Now()

// MonotonicNowUs is only a process-relative fallback on non-Linux systems
// (used by tests); DANOS runs on Linux.
func MonotonicNowUs() int64 {
	return time.Since(processStart).Microseconds()
}
