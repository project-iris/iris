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

// Contains the logic for handling the core iris command.

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"flag"
	"fmt"
	"github.com/karalabe/iris/proto/carrier"
	"github.com/karalabe/iris/service/relay"
	"log"
	rng "math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
)

// Flag set for the core iris command.
var irisFlags = flag.NewFlagSet("iris", flag.ExitOnError)

// Prints the usage of the iris command and its options.
func irisUsage() {
	fmt.Printf("Server node of the Iris decentralized messaging framework.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("\t%s [command] [options]\n\n", os.Args[0])

	fmt.Println("Developer mode:")
	irisFlags.VisitAll(func(f *flag.Flag) {
		if f.Name == "dev" {
			if f.DefValue != "" {
				fmt.Printf("\t-%-12s%-12s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
			} else {
				fmt.Printf("\t-%-20s%s\n", f.Name, f.Usage)
			}
		}
	})
	fmt.Printf("\n")

	fmt.Printf("Production options:\n")
	irisFlags.VisitAll(func(f *flag.Flag) {
		if !strings.HasSuffix(f.Name, "prof") && f.Name != "dev" {
			if f.DefValue != "" {
				fmt.Printf("\t-%-12s%-12s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
			} else {
				fmt.Printf("\t-%-24s%s\n", f.Name, f.Usage)
			}
		}
	})
	fmt.Printf("\n")

	fmt.Printf("Profiling options:\n")
	irisFlags.VisitAll(func(f *flag.Flag) {
		if strings.HasSuffix(f.Name, "prof") {
			fmt.Printf("\t-%-24s%s\n", f.Name, f.Usage)
		}
	})
	fmt.Printf("\n")

	//fmt.Printf("Extending services:\n")
	//fmt.Printf("\n")

	fmt.Printf("Helper commands:\n")
	fmt.Printf("\t%-25s%s\n", "gateway add", "adds a remote network to the local gateway")
	fmt.Printf("\t%-25s%s\n", "gateway init", "initializes a local gateway configuration")
	fmt.Printf("\t%-25s%s\n", "gateway rm", "removes a remote network from the local gateway")
	fmt.Printf("\nUse \"%s [command] --help\" for more information about that command.\n\n", os.Args[0])
}

// Parses the iris command line flags and checks their validity.
func parseIrisFlags() (int, string, *rsa.PrivateKey, string, string) {
	var rsaKey *rsa.PrivateKey

	// Assign and parse the command line flags
	devMode := irisFlags.Bool("dev", false, "start in local developer mode, isolated from remote peers")

	networkName := irisFlags.String("net", "", "name of the network to join or create")
	rsaKeyPath := irisFlags.String("rsa", "", "path to the RSA private key to use for data security")
	relayPort := irisFlags.Int("relay", 55555, "relay endpoint for locally connecting clients")
	//fedConfPath := irisFlags.String("gateway", "", "path to the gateway configuration")

	cpuProfile := irisFlags.String("cpuprof", "", "path to CPU profiling results")
	blockProfile := irisFlags.String("blockprof", "", "path to lock contention profiling results")

	irisFlags.Usage = irisUsage
	irisFlags.Parse(os.Args[1:])

	// Check the relay port range
	if *relayPort <= 0 || *relayPort >= 65536 {
		fatal("Invalid relay port: have %v, want [1-65535].", *relayPort)
	}
	// User random network id and RSA key in developer mode
	if *devMode {
		// Generate a secure RSA key
		fmt.Printf("Entering developer mode\n")
		fmt.Printf("Generating random RSA key... ")
		if key, err := rsa.GenerateKey(rand.Reader, 2048); err != nil {
			fatal("failed: %v", err)
		} else {
			fmt.Printf("done.\n")
			rsaKey = key
		}
		// Generate a probably unique network name
		fmt.Printf("Generating random network name... ")
		*networkName = fmt.Sprintf("dev-network-%v", rng.Int63())
		fmt.Printf("done.\n")
		fmt.Println()
	} else {
		// Production mode, read the network id and RSA key from the arguments
		if *networkName == "" {
			fatal("No network specified (-net), did you intend developer mode (-dev)?", irisUsage)
		}
		if *rsaKeyPath == "" {
			fatal("No RSA key specified (-rsa), did you intend developer mode (-dev)?", irisUsage)
		}
		if key, err := parseRsaKey(*rsaKeyPath); err != nil {
			fatal("Loading RSA key failed: %v.", err)
		} else {
			rsaKey = key
		}
	}
	return *relayPort, *networkName, rsaKey, *cpuProfile, *blockProfile
}

// Program entry point for running the Iris node.
func irisMain() {
	relayPort, networkId, rsaKey, cpuProfile, blockProfile := parseIrisFlags()

	// Check for CPU profiling
	if cpuProfile != "" {
		prof, err := os.Create(cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(prof)
		defer pprof.StopCPUProfile()
	}
	// Check for lock contention profiling
	if blockProfile != "" {
		prof, err := os.Create(blockProfile)
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
