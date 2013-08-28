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
)

// Flag sets for the gateway subcommands.
var gateInitFlags = flag.NewFlagSet("iris gateway init", flag.ExitOnError)
var gateAddFlags = flag.NewFlagSet("iris gateway add", flag.ExitOnError)

func gateInitUsage() {
	fmt.Printf("Creates a new gateway configuration for federated networks.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("\t%s gateway new [options]\n\n", os.Args[0])

	fmt.Printf("Command options:\n")
	gateInitFlags.VisitAll(func(f *flag.Flag) {
		if f.DefValue != "" {
			fmt.Printf("\t-%-8s%-20s%s\n", f.Name, "[="+f.DefValue+"]", f.Usage)
		} else {
			fmt.Printf("\t-%-28s%s\n", f.Name, f.Usage)
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
	gateConfPath := gateInitFlags.String("gate", "gateway.json", "gateway config file to initialize")
	netConfPath := gateInitFlags.String("remote", "<network>.json", "network config file to create")

	gateInitFlags.Usage = gateInitUsage
	gateInitFlags.Parse(os.Args[3:])

	// Validate the command line arguments
	if *networkName == "" {
		fmt.Fprintf(os.Stderr, "You must specify a network name (-net)!\n")
		gateInitUsage()
		os.Exit(1)
	}
	if *netConfPath == "<net name>.json" {
		*netConfPath = *networkName + ".json"
	}
	if *rsaKeyPath == "" {
		fmt.Fprintf(os.Stderr, "You must specify a private key (-rsa)!\n")
		gateInitUsage()
		os.Exit(1)
	}
	key, err := parseRsaKey(*rsaKeyPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Loading RSA key failed: %v.\n", err)
		os.Exit(1)
	}
	if *gateAddr == "" {
		fmt.Fprintf(os.Stderr, "You must specify a local listener address for the gateway (-addr)!\n")
		gateInitUsage()
		os.Exit(1)
	}
	addr, err := net.ResolveTCPAddr("tcp", *gateAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid gateway listener address: %v.\n", err)
		os.Exit(1)
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
		ioutil.WriteFile(gatePath, config, 0640)
		fmt.Printf("Initialized empty gateway config file \"%s\":\n%s\n", gatePath, config)
		fmt.Printf("You can insert remote networks into it with \"%s gateway add\"\n\n", os.Args[0])
	}
	if config, err := gateway.InitNetworkConfig(id, key); err != nil {
		panic(err)
	} else {
		ioutil.WriteFile(netPath, config, 0640)
		fmt.Printf("Initialized network config file \"%s\":\n%s\n", netPath, config)
		fmt.Printf("You can insert it into remote gateway configs with \"%s gateway add\"\n\n", os.Args[0])
	}
}

func gateAddUsage() {

}

// Parses the iris command line flags and checks their validity.
func parseGateAddFlags() {
	// Assign the command line flags
	//rsaKeyPath := gateAddFlags.String("rsa", "", "local RSA private key to sign with")
	//gateConfPath := gateAddFlags.String("gateway", "", "local gateway config file to add to")
	//netConfPath := gateAddFlags.String("remote", "", "remote network config file to insert")

	//gateAddFlags.Usage = gateAddUsage
	gateAddFlags.Parse(os.Args[3:])
}

func gateAddMain() {

}
