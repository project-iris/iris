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

// Contains the address scanning ad-hoc seed generator. It continuously returns
// IP addresses up- and downwards from the current host address within the given
// network subnet.

package bootstrap

import (
	"fmt"
	"net"

	"gopkg.in/inconshreveable/log15.v2"
)

// Ad-hoc address scanning seed generator.
type scanSeeder struct {
	ipnet *net.IPNet      // IP network assigned to the seed generator
	quit  chan chan error // Quit channel to synchronize termination
	log   log15.Logger    // Contextual logger with injected ipnet and algorithm
}

// Creates a new scanning seed generator.
func newScanSeeder(ipnet *net.IPNet, logger log15.Logger) seeder {
	return &scanSeeder{
		ipnet: ipnet,
		quit:  make(chan chan error),
		log:   logger.New("algo", "scan"),
	}
}

// Starts the seed generator.
func (s *scanSeeder) Start(sink chan *net.IPAddr, phase *uint32) error {
	go s.run(sink, phase)
	return nil
}

// Terminates the seed generator.
func (s *scanSeeder) Close() error {
	errc := make(chan error, 1)
	s.quit <- errc
	return <-errc
}

// Generates IP addresses in the network linearly from the current address.
func (s *scanSeeder) run(sink chan *net.IPAddr, phase *uint32) {
	s.log.Info("starting seed generator")
	var errc chan error
	var err error

	// Split the IP address into subnet and host parts
	subnetBits, maskBits := s.ipnet.Mask.Size()
	hostBits := maskBits - subnetBits

	hostIP := 0
	for i := 0; i < hostBits; i++ {
		hostIP += int(s.ipnet.IP[len(s.ipnet.IP)-1-i/8]) & (1 << uint(i%8))
	}
	// Make sure the specified IP net can be scanned (avoid point-to-point interfaces)
	if hostBits < 2 {
		err = fmt.Errorf("host address space too small: %v bits", hostBits)
	}
	// Loop until an error occurs or closure is requested
	for up, down, offset := true, true, 0; err == nil && errc == nil; {
		// If the address space was fully scanned, reset
		if !up && !down {
			up, down, offset = true, true, 0
		}
		// Generate the next host IP segment and update the offset
		nextIP := hostIP + offset
		offset = -offset
		if offset >= 0 {
			offset++
		}
		// Make sure we didn't run out of the subnet (ignore subnet and broadcast address)
		if nextIP <= 0 {
			down = false
			continue
		}
		if nextIP >= (1<<uint(hostBits))-1 {
			up = false
			continue
		}
		// Generate the full host address and send it upstream
		host := s.ipnet.IP.Mask(s.ipnet.Mask)
		for i := len(host) - 1; i >= 0; i-- {
			host[i] |= byte(nextIP & 255)
			nextIP >>= 8
		}
		select {
		case sink <- &net.IPAddr{IP: host}:
		case errc = <-s.quit:
		}
	}
	// Log termination status, wait until closure request and return
	if err != nil {
		s.log.Error("seeder terminating prematurely", "error", err)
	} else {
		s.log.Info("seeder terminating gracefully")
	}
	if errc == nil {
		errc = <-s.quit
	}
	errc <- err
}
