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

// Package system implements some basic operating system related tasks.
package system

import (
	"sync"
	"time"

	"github.com/project-iris/iris/config"
)

// Cpu usage infos and statistics (not much needed for now).
type cpuInfo struct {
	usage float32

	lock sync.RWMutex
}

// Singleton CPU status file.
var cpu cpuInfo

// Returns the CPU usage since the last measurement cycle.
func CpuUsage() float32 {
	cpu.lock.RLock()
	defer cpu.lock.RUnlock()
	return cpu.usage
}

// Init function to start the measurements
func init() {
	// Make sure state is initialized to something
	gatherCpuInfo()
	time.Sleep(100 * time.Millisecond)
	gatherCpuInfo()

	// Measure till program is terminated
	go func() {
		tick := time.Tick(config.ScribeBeatPeriod)
		for {
			<-tick
			gatherCpuInfo()
		}
	}()
}
