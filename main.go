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

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	rng "math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"

	"github.com/project-iris/iris/proto/iris"
	"github.com/project-iris/iris/service/relay"
)

// Command line flags
var devMode = flag.Bool("dev", false, "start in local developer mode (random cluster and key)")
var relayPort = flag.Int("port", 55555, "relay endpoint for locally connecting clients")
var clusterName = flag.String("net", "", "name of the cluster to join or create")
var rsaKeyPath = flag.String("rsa", "", "path to the RSA private key to use for data security")

var cpuProfile = flag.String("cpuprof", "", "path to CPU profiling results")
var heapProfile = flag.String("heapprof", "", "path to memory heap profiling results")
var blockProfile = flag.String("blockprof", "", "path to lock contention profiling results")

// Prints the usage of the Iris command and its options.
func usage() {
	fmt.Printf("Server node of the Iris decentralized messaging framework.\n\n")
	fmt.Printf("Usage:\n\n")
	fmt.Printf("\t%s [options]\n\n", os.Args[0])

	fmt.Printf("The options are:\n\n")
	flag.VisitAll(func(f *flag.Flag) {
		if !strings.HasSuffix(f.Name, "prof") {
			if f.DefValue != "" {
				fmt.Printf("\t-%-8s%-12s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
			} else {
				fmt.Printf("\t-%-20s%s\n", f.Name, f.Usage)
			}
		}
	})
	fmt.Printf("\n")

	fmt.Printf("Profiling options:\n\n")
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasSuffix(f.Name, "prof") {
			fmt.Printf("\t-%-20s%s\n", f.Name, f.Usage)
		}
	})
	fmt.Printf("\n")
}

// Parses the command line flags and checks their validity
func parseFlags() (int, string, *rsa.PrivateKey) {
	var rsaKey *rsa.PrivateKey

	// Read the command line arguments
	flag.Usage = usage
	flag.Parse()

	// Check the relay port range
	if *relayPort <= 0 || *relayPort >= 65536 {
		fmt.Fprintf(os.Stderr, "Invalid relay port: have %v, want [1-65535].\n", *relayPort)
		os.Exit(-1)
	}
	// User random cluster id and RSA key in developer mode
	if *devMode {
		// Generate a secure RSA key
		fmt.Printf("Entering developer mode\n")
		fmt.Printf("Generating random RSA key... ")
		if key, err := rsa.GenerateKey(rand.Reader, 2048); err != nil {
			fmt.Printf("failed: %v\n", err)
			os.Exit(-2)
		} else {
			fmt.Printf("done.\n")
			rsaKey = key
		}
		// Generate a probably unique cluster name
		fmt.Printf("Generating random cluster name... ")
		*clusterName = fmt.Sprintf("dev-cluster-%v", rng.Int63())
		fmt.Printf("done.\n")
		fmt.Println()
	} else {
		// Production mode, read the cluster id and RSA key from teh arguments
		if *clusterName == "" {
			fmt.Fprintf(os.Stderr, "No cluster specified (-net), did you intend developer mode (-dev)?\n")
			os.Exit(-1)
		}
		if *rsaKeyPath == "" {
			fmt.Fprintf(os.Stderr, "No RSA key specified (-rsa), did you intend developer mode (-dev)?\n")
			os.Exit(-1)
		}
		if rsaData, err := ioutil.ReadFile(*rsaKeyPath); err != nil {
			fmt.Fprintf(os.Stderr, "Reading RSA key failed: %v.\n", err)
			os.Exit(-1)
		} else {
			// Try processing as PEM format
			if block, _ := pem.Decode(rsaData); block != nil {
				if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
					fmt.Fprintf(os.Stderr, "Parsing RSA key from PEM format failed: %v.\n", err)
					os.Exit(-1)
				} else {
					rsaKey = key
				}
			} else {
				// Give it a shot as simple binary DER
				if key, err := x509.ParsePKCS1PrivateKey(rsaData); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to parse RSA key from both PEM and DER format.\n")
					os.Exit(-1)
				} else {
					rsaKey = key
				}
			}
		}
	}
	return *relayPort, *clusterName, rsaKey
}

func main() {
	// Extract the command line arguments
	relayPort, clusterId, rsaKey := parseFlags()

	// Check for CPU profiling
	if *cpuProfile != "" {
		prof, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(prof)
		defer pprof.StopCPUProfile()
	}
	// Check for memory profiling
	if *heapProfile != "" {
		prof, err := os.Create(*heapProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.Lookup("heap").WriteTo(prof, 0)
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
	// Set the concurrency level
	runtime.GOMAXPROCS(4 * runtime.NumCPU())

	// Create and boot a new carrier
	log.Printf("main: booting iris overlay...")
	overlay := iris.New(clusterId, rsaKey)
	if peers, err := overlay.Boot(); err != nil {
		log.Fatalf("main: failed to boot iris overlay: %v.", err)
	} else {
		log.Printf("main: iris overlay converged with %v remote connections.", peers)
	}
	// Create and boot a new relay
	log.Printf("main: booting relay service...")
	rel, err := relay.New(relayPort, overlay)
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
		log.Printf("main: failed to terminate relay service: %v.", err)
	}
	log.Printf("main: terminating carrier...")
	if err := overlay.Shutdown(); err != nil {
		log.Printf("main: failed to shutdown iris overlay: %v.", err)
	}
	log.Printf("main: iris terminated.")
}
