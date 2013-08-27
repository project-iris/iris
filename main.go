// Iris - Decentralized Messaging Framework
// Copyright 2013 Peter Szilagyi. All rights reserved.
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
//
// Author: peterke@gmail.com (Peter Szilagyi)

package main

import (
	"github.com/karalabe/iris/proto/carrier"
	"github.com/karalabe/iris/service/relay"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
)

func main() {
	// Extract the command line arguments
	relayPort, networkId, rsaKey := parseFlags()

	// Check for CPU profiling
	if *cpuProfile != "" {
		prof, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(prof)
		defer pprof.StopCPUProfile()
	}
	// Check for lock contention profiling
	if *blockProfile != "" {
		prof, err := os.Create(*blockProfile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
		defer pprof.Lookup("block").WriteTo(prof, 0)
	}

	// Create and boot a new carrier
	log.Printf("main: booting carrier...")
	car := carrier.New(networkId, rsaKey)
	if peers, err := car.Boot(); err != nil {
		log.Fatalf("main: failed to boot carrier: %v.", err)
	} else {
		log.Printf("main: carrier converged with %v remote connections.", peers)
	}
	// Create and boot a new relay
	log.Printf("main: booting relay service...")
	rel, err := relay.New(relayPort, car)
	if err != nil {
		log.Fatalf("main: failed to create relay service: %v.", err)
	}
	if err := rel.Boot(); err != nil {
		log.Fatalf("main: failed to boot relay: %v.", err)
	}

	// Capture termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Report success
	log.Printf("main: iris successfully booted, listening on port %d.", relayPort)

	// Wait for termination request, clean up and exit
	<-quit
	log.Printf("main: terminating relay service...")
	if err := rel.Terminate(); err != nil {
		log.Printf("main: relay service termination failure: %v.", err)
	}
	log.Printf("main: terminating carrier...")
	car.Shutdown()
	log.Printf("main: iris terminated.")
}
