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

// This file contains the CPU measurements for Linux and related OSes.

package system

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Intermediate CPU statistics for the Linux platform.
type cpuInfoLinux struct {
	user int
	nice int
	sys  int
	idle int
}

// Intermediate CPU statistics for the Linux platform.
var cpuLinux cpuInfoLinux

// Gathers CPU statistics and fills the global cpu stat variable.
func gatherCpuInfo() {
	// Open the cpu stat file
	inf, err := os.Open("/proc/stat")
	if err != nil {
		panic(err)
	}
	in := bufio.NewReader(inf)

	// Read all the overall CPU usage numbers
	for found := false; !found; {
		line, err := in.ReadString('\n')
		if err != nil {
			panic(err)
		}
		if line[0:4] != "cpu " {
			continue
		}
		found = true

		// Extract the CPU counters
		var user, nice, sys, idle int
		fmt.Fscan(strings.NewReader(line[4:]), &user, &nice, &sys, &idle)

		// Update the local and global state
		cpu.lock.Lock()
		prev := cpuLinux.user + cpuLinux.nice + cpuLinux.sys
		busy := user + nice + sys

		work := busy - prev
		wait := idle - cpuLinux.idle

		cpu.usage = float32(work) / float32(work+wait)
		cpuLinux.user = user
		cpuLinux.nice = nice
		cpuLinux.idle = idle
		cpuLinux.sys = sys
		cpu.lock.Unlock()
	}
}
