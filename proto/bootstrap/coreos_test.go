// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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

package bootstrap

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/test/docker"
	"gopkg.in/inconshreveable/log15.v2"
)

// Docker container of the CoreOS etcd service
const etcd = "coreos/etcd"

// Tests that the CoreOS seeder can indeed retrieve membership lists from a
// locally running etcd service and filter them by bootstrap interface.
func TestCoreOSSeeder(t *testing.T) {
	// Skip test in short mode (i.e. no docker, no test)
	if testing.Short() {
		t.Log("Skipping test in short mode.")
		return
	}
	// Make sure test prerequisites are available
	if err := docker.CheckInstallation(); err != nil {
		t.Fatalf("docker installation not found: %v.", err)
	}
	if ok, err := docker.CheckImage(etcd); err != nil {
		t.Fatalf("failed to check etcd container availability: %v.", err)
	} else if !ok {
		t.Logf("pulling %s from docker registry...", etcd)
		if err := docker.PullImage(etcd); err != nil {
			t.Fatalf("failed to pull etcd container: %v.", err)
		}
	}
	// Start the CoreOS/etcd service and ensure cleanup
	flags := []string{}
	for _, port := range config.BootCoreOSPorts {
		flags = append(flags, "-p")
		flags = append(flags, fmt.Sprintf("%d:%d", port, port))
	}
	container, err := docker.StartContainer(flags, etcd)
	if err != nil {
		t.Fatalf("failed to start etcd container: %v.", err)
	}
	defer docker.CloseContainer(container)

	// Get the IPNet of the localhost
	addr, _ := net.ResolveIPAddr("ip", "127.0.0.1")
	ipnet := &net.IPNet{
		IP:   addr.IP,
		Mask: addr.IP.DefaultMask(),
	}
	// Create the CoreOS seed generator, address sink and boot it
	seeder := newCoreOSSeeder(ipnet, log15.New("ipnet", ipnet))
	sink, phase := make(chan *net.IPAddr), uint32(0)

	if err := seeder.Start(sink, &phase); err != nil {
		t.Fatalf("failed to start seed generator: %v.", err)
	}
	// Wait a while and check generated seed list
	for i := 0; i < 3; i++ {
		select {
		case addr := <-sink:
			// Make sure localhost is the one found
			if !addr.IP.IsLoopback() {
				t.Fatalf("non-filtered seed returned: %v.", addr)
			}
		case <-time.After(2 * config.BootCoreOSFastRescan):
			t.Fatalf("failed to find peers in %v.", 2*config.BootCoreOSFastRescan)
		}
		// Make sure that nothing else is found yet
		select {
		case addr := <-sink:
			t.Fatalf("unexpected seed returned: %v.", addr)
		default:
		}
	}
	// Terminate the generator
	if err := seeder.Close(); err != nil {
		t.Fatalf("failed to terminate seed generator: %v.", err)
	}
}
