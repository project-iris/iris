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
