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

// Contains the command line flags and their processing logic.

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	rng "math/rand"
	"os"
	"strings"
)

// Command line flags
var devMode = flag.Bool("dev", false, "start in local developer mode, isolated from remote peers")
var relayPort = flag.Int("relay", 55555, "relay endpoint for locally connecting clients")
var networkName = flag.String("network", "", "name of the network to join or create")
var rsaKeyPath = flag.String("rsa", "", "path to the RSA private key to use for data security")
var fedConfPath = flag.String("federation", "", "path to the federation configuration")

var cpuProfile = flag.String("cpuprof", "", "path to CPU profiling results")
var blockProfile = flag.String("blockprof", "", "path to lock contention profiling results")

// Prints the usage of the Iris command and its options.
func usage() {
	fmt.Printf("Server node of the Iris decentralized messaging framework.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("\t%s [options]\n\n", os.Args[0])

	fmt.Println("Developer mode:")
	flag.VisitAll(func(f *flag.Flag) {
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
	flag.VisitAll(func(f *flag.Flag) {
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
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasSuffix(f.Name, "prof") {
			fmt.Printf("\t-%-24s%s\n", f.Name, f.Usage)
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
	// User random network id and RSA key in developer mode
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
		// Generate a probably unique network name
		fmt.Printf("Generating random network name... ")
		*networkName = fmt.Sprintf("dev-network-%v", rng.Int63())
		fmt.Printf("done.\n")
		fmt.Println()
	} else {
		// Production mode, read the network id and RSA key from the arguments
		if *networkName == "" {
			fmt.Fprintf(os.Stderr, "No network specified (-net), did you intend developer mode (-dev)?\n")
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
	return *relayPort, *networkName, rsaKey
}
