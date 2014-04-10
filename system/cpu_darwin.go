// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
//
// Iris is dual licensed: you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation, either version 3 of the License, or (at your option) any later
// version.
//
// The framework is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
// more details.
//
// Alternatively, the Iris framework may be used in accordance with the terms
// and conditions contained in a signed written agreement between you and the
// author(s).

// This file contains the CPU measurements for Mac OS X.

package system

/*
#include <mach/mach_init.h>
#include <mach/mach_error.h>
#include <mach/mach_host.h>
#include <mach/vm_map.h>

static host_cpu_load_info_data_t cpuInfo;

static inline int CollectCpuInfo() {
	mach_msg_type_number_t infoCount = HOST_CPU_LOAD_INFO_COUNT;
	return host_statistics(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info_t)&cpuInfo, &infoCount) == KERN_SUCCESS;
}

static inline long long TotalTime() {
	long long total = 0;

	int i;
	for (i=0; i<CPU_STATE_MAX; i++) {
		total += cpuInfo.cpu_ticks[i];
	}
	return total;
}

static inline long long IdleTime() {
	return cpuInfo.cpu_ticks[CPU_STATE_IDLE];
}
*/
import "C"
import "fmt"

// Intermediate CPU statistics for the Windows platform.
type cpuInfoDarwin struct {
	total int64
	idle  int64
}

// Intermediate CPU statistics for the Windows platform.
var cpuDarwin cpuInfoDarwin

// Gathers CPU statistics and fills the global cpu stat variable.
func gatherCpuInfo() {
	// Collect the current usage info
	if C.CollectCpuInfo() == 0 {
		panic(fmt.Errorf("faield to gther CPU usage info"))
	}
	// Extract the CPU counters
	total := int64(C.TotalTime())
	idle := int64(C.IdleTime())

	// Update the local and global state
	cpu.lock.Lock()
	defer cpu.lock.Unlock()

	prev := cpuDarwin.total + -cpuDarwin.idle
	busy := total - idle

	work := busy - prev
	wait := idle - cpuDarwin.idle

	cpu.usage = float32(work) / float32(work+wait)
	cpuDarwin.total = total
	cpuDarwin.idle = idle
}
