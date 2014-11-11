// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
//
// Community license: for open source projects and services, Iris is free to use,
// redistribute and/or modify under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation, either version 3, or (at
// your option) any later version.
//
// Evaluation license: you are free to privately evaluate Iris without adhering
// to either of the community or commercial licenses for as long as you like,
// however you are not permitted to publicly release any software or service
// built on top of it without a valid license.
//
// Commercial license: for commercial and/or closed source projects and services,
// the Iris cloud messaging system may be used in accordance with the terms and
// conditions contained in an individually negotiated signed written agreement
// between you and the author(s).

// This file contains the CPU measurements for Windows.

package system

/*
#include <windows.h>

static FILETIME userTime;
static FILETIME kernelTime;
static FILETIME idleTime;

static inline int CollectCpuInfo() {
	return GetSystemTimes(&idleTime, &kernelTime, &userTime);
}

static inline long long UserTime() {
	return ((long long)userTime.dwHighDateTime) << 32 | userTime.dwLowDateTime;
}

static inline long long KernelTime() {
	return ((long long)kernelTime.dwHighDateTime) << 32 | kernelTime.dwLowDateTime;
}

static inline long long IdleTime() {
	return ((long long)idleTime.dwHighDateTime) << 32 | idleTime.dwLowDateTime;
}
*/
import "C"
import "fmt"

// Intermediate CPU statistics for the Windows platform.
type cpuInfoWindows struct {
	user   int64
	kernel int64
	idle   int64
}

// Intermediate CPU statistics for the Windows platform.
var cpuWindows cpuInfoWindows

// Gathers CPU statistics and fills the global cpu stat variable.
func gatherCpuInfo() {
	// Collect the current usage info
	if C.CollectCpuInfo() == 0 {
		panic(fmt.Errorf("faield to gther CPU usage info"))
	}
	// Extract the CPU counters
	user := int64(C.UserTime())
	kernel := int64(C.KernelTime())
	idle := int64(C.IdleTime())

	// Update the local and global state
	cpu.lock.Lock()
	defer cpu.lock.Unlock()

	prev := cpuWindows.user + cpuWindows.kernel - cpuWindows.idle
	busy := user + kernel - idle

	work := busy - prev
	wait := idle - cpuWindows.idle

	cpu.usage = float32(work) / float32(work+wait)
	cpuWindows.user = user
	cpuWindows.kernel = kernel
	cpuWindows.idle = idle
}
