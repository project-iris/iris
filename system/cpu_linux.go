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
