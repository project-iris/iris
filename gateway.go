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

// Contains the logic for handling the gateway subcommands.

package main

import (
	"crypto/rsa"
	"flag"
	"fmt"
	"github.com/karalabe/iris/service/gateway"
	"io/ioutil"
	"net"
	"os"
	"regexp"
)

// Flag sets for the gateway subcommands.
var gateInitFlags = flag.NewFlagSet("iris gateway init", flag.ExitOnError)
var gateAddFlags = flag.NewFlagSet("iris gateway add", flag.ExitOnError)
var gateRmFlags = flag.NewFlagSet("iris gateway rm", flag.ExitOnError)

// Prints the usage of the gateway init subcommand and its options.
func gateInitUsage() {
	fmt.Printf("Creates a new gateway configuration for federated networks.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("\t%s gateway init [options]\n\n", os.Args[0])

	fmt.Printf("Command options:\n")
	gateInitFlags.VisitAll(func(f *flag.Flag) {
		if f.DefValue != "" {
			fmt.Printf("\t-%-12s%-12s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
		} else {
			fmt.Printf("\t-%-24s%s\n", f.Name, f.Usage)
		}
	})
	fmt.Printf("\n")
}

// Parses the iris command line flags and checks their validity.
func parseGateInitFlags() (string, *rsa.PrivateKey, *net.TCPAddr, string, string) {
	// Assign and parse the command line flags
	networkName := gateInitFlags.String("net", "", "network name of the local cluster")
	rsaKeyPath := gateInitFlags.String("rsa", "", "private key of the local cluster")
	gateAddr := gateInitFlags.String("addr", "", "gateway listener address")
	gateConfPath := gateInitFlags.String("gate", "", "gateway config file to initialize")
	netConfPath := gateInitFlags.String("remote", "", "network config file to create")

	gateInitFlags.Usage = gateInitUsage
	gateInitFlags.Parse(os.Args[3:])

	// Validate the command line arguments
	if *networkName == "" {
		fatal("You must specify a network name (-net)!", gateInitUsage)
	}
	if *rsaKeyPath == "" {
		fatal("You must specify a private key (-rsa)!", gateInitUsage)
	}
	key, err := parseRsaKey(*rsaKeyPath)
	if err != nil {
		fatal("Loading RSA key failed: %v.", err)
	}
	if *gateAddr == "" {
		fatal("You must specify a local listener address for the gateway (-addr)!", gateInitUsage)
	}
	addr, err := net.ResolveTCPAddr("tcp", *gateAddr)
	if err != nil {
		fatal("Invalid gateway listener address: %v.", err)
	}
	return *networkName, key, addr, *gateConfPath, *netConfPath
}

// Initializes new gateway and network config files.
func gateInitMain() {
	id, key, addr, gatePath, netPath := parseGateInitFlags()

	// Create the configuration blobs
	if config, err := gateway.InitGatewayConfig(addr); err != nil {
		panic(err)
	} else {
		if gatePath != "" {
			if err := ioutil.WriteFile(gatePath, config, 0640); err != nil {
				fatal("Failed to export gateway config \"%s\": %v.", gatePath, err)
			} else {
				fmt.Printf("Initialized gateway config, exported into \"%s\"\n", gatePath)
			}
		} else {
			fmt.Printf("Initialized gateway config, export not requested (-gate)\n")
		}
		fmt.Printf("%s\n", config)
		fmt.Printf("You can insert remote networks into it with \"%s gateway add\"\n\n", os.Args[0])
	}
	if config, err := gateway.InitNetworkConfig(id, key); err != nil {
		panic(err)
	} else {
		if netPath != "" {
			if err := ioutil.WriteFile(netPath, config, 0640); err != nil {
				fatal("Failed to export network config \"%s\": %v.", netPath, err)
			} else {
				fmt.Printf("Initialized network config, exported into \"%s\"\n", netPath)
			}
		} else {
			fmt.Printf("Initialized network config, export not requested (-remote)\n")
		}
		fmt.Printf("%s\n", config)
		fmt.Printf("You can insert it into remote gateway configs with \"%s gateway add\"\n\n", os.Args[0])
	}
}

// Prints the usage of the gateway add subcommand and its options.
func gateAddUsage() {
	fmt.Printf("Adds a remote network into the local new gateway configuration.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("\t%s gateway add [options] <access-points...>\n\n", os.Args[0])

	fmt.Printf("Command options:\n")
	gateAddFlags.VisitAll(func(f *flag.Flag) {
		if f.DefValue != "" {
			fmt.Printf("\t-%-12s%-12s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
		} else {
			fmt.Printf("\t-%-24s%s\n", f.Name, f.Usage)
		}
	})
	fmt.Printf("\n")
}

// Parses the iris command line flags and checks their validity.
func parseGateAddFlags() (*rsa.PrivateKey, string, string, string, []string) {
	// Assign and parse the command line flags
	rsaKeyPath := gateAddFlags.String("rsa", "", "private key of the local cluster")
	gateConfPath := gateAddFlags.String("gate", "", "gateway config file to insert into")
	netConfPath := gateAddFlags.String("remote", "", "network config file to insert")
	accessPerm := gateAddFlags.String("access", "", "access permissions of the remote network")

	gateAddFlags.Usage = gateAddUsage
	gateAddFlags.Parse(os.Args[3:])

	// Validate the command line arguments
	if *rsaKeyPath == "" {
		fatal("You must specify a private key (-rsa)!", gateAddUsage)
	}
	key, err := parseRsaKey(*rsaKeyPath)
	if err != nil {
		fatal("Loading RSA key failed: %v.", err)
	}
	if *gateConfPath == "" {
		fatal("You must specify a gateway configuration to modify (-gate)!", gateAddUsage)
	}
	if *netConfPath == "" {
		fatal("You must specify a network configuration to insert (-remote)!", gateAddUsage)
	}
	if *accessPerm == "" {
		fatal("You must specify the access permissions (-access)!", gateAddUsage)
	}
	if _, err := regexp.Compile(*accessPerm); err != nil {
		fatal("Invalid access pattern: %v.", err)
	}
	// Extract the list of gateways and verify their validity
	gates := gateAddFlags.Args()
	if len(gates) == 0 {
		fatal("You must specify at least one remote access point!", gateAddUsage)
	}
	for _, gate := range gates {
		if _, err := net.ResolveTCPAddr("tcp", gate); err != nil {
			fatal("Invalid gateway address: %v.", err)
		}
	}
	// Return the parsed values
	return key, *gateConfPath, *netConfPath, *accessPerm, gates
}

// Merges a new network into a gateway config file.
func gateAddMain() {
	key, gatePath, netPath, access, gates := parseGateAddFlags()

	// Load the gateway and network config files
	gateData, err := ioutil.ReadFile(gatePath)
	if err != nil {
		fatal("Failed to load gateway config file: %v.", err)
	}
	netData, err := ioutil.ReadFile(netPath)
	if err != nil {
		fatal("Failed to load network config file: %v.", err)
	}
	// Insert the network into the gateway config
	config, err := gateway.AddNetwork(gateData, netData, access, gates, key)
	if err != nil {
		fatal("Failed to merge configs: %v.", err)
	}
	// Serialize it back to disk and report
	if err := ioutil.WriteFile(gatePath, config, 0640); err != nil {
		fatal("Failed to export gateway config \"%s\": %v.", gatePath, err)
	} else {
		fmt.Printf("Merged gateway config, exported into \"%s\"\n", gatePath)
	}
	fmt.Printf("%s\n\n", config)
}
