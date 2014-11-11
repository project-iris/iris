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

// Contains the CoreOS/etcd based seed generator. It seamlessly tries to connect
// to a locally running etcd instance and use it as a seed server.

package bootstrap

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/project-iris/iris/config"
	"gopkg.in/inconshreveable/log15.v2"
)

// Regexp pattern to extract peer IPs from peer URLs.
var peerPattern = regexp.MustCompile("http://([0-9\\.:]+):[0-9]+$")

// Membership data returned by etcd.
type coreOSMember struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	ClientUrl string `json:"clientUrl"`
	PeerUrl   string `json:"peerUrl"`
}

// CoreOS/etcd service based seed generator.
type coreOSSeeder struct {
	ipnet *net.IPNet      // IP network assigned to the seed generator
	quit  chan chan error // Quit channel to synchronize termination
	log   log15.Logger    // Contextual logger with injected ipnet and algorithm
}

// Creates a new CoreOS seed generator.
func newCoreOSSeeder(ipnet *net.IPNet, logger log15.Logger) seeder {
	return &coreOSSeeder{
		ipnet: ipnet,
		quit:  make(chan chan error),
		log:   logger.New("algo", "coreos"),
	}
}

// Starts the seed generator.
func (s *coreOSSeeder) Start(sink chan *net.IPAddr, phase *uint32) error {
	go s.run(sink, phase)
	return nil
}

// Terminates the seed generator.
func (s *coreOSSeeder) Close() error {
	errc := make(chan error, 1)
	s.quit <- errc
	return <-errc
}

// Periodically retrieves the CoreOS cluster membership infos and returns local
// addresses to the bootstrapper.
func (s *coreOSSeeder) run(sink chan *net.IPAddr, phase *uint32) {
	s.log.Info("starting seed generator")

	var errc chan error
	var err error

	// Loop until an error occurs or closure is requested
	for trials := 0; err == nil && errc == nil; {
		// Assemble a list of all possible etcd endpoints
		endpoints := []string{}
		for _, ip := range []string{"127.0.0.1", "[::1]"} {
			for _, port := range config.BootCoreOSPorts {
				endpoints = append(endpoints, fmt.Sprintf("%s:%d", ip, port))
			}
		}
		// Try and retrieve membership infos from all probable etcd endpoints
		errs, peers := []error{}, make(map[string]struct{})
		for _, addr := range endpoints {
			// Try and retrieve the member list from etcd
			res, err := http.Get(fmt.Sprintf("http://%s/v2/admin/machines", addr))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			// Read and parse the returned list
			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			var members []coreOSMember
			if err := json.Unmarshal(body, &members); err != nil {
				errs = append(errs, err)
				continue
			}
			// Extract the remote IP addresses
			for _, member := range members {
				match := peerPattern.FindStringSubmatch(member.PeerUrl)
				if len(match) == 2 {
					peers[match[1]] = struct{}{}
				}
			}
		}
		// If no IPs have been found, log a message and retry after a while
		if len(peers) == 0 {
			trials++
			sleep := time.Duration(trials) * config.BootCoreOSSleepIncrement
			if sleep > config.BootCoreOSSleepLimit {
				sleep = config.BootCoreOSSleepLimit
			}
			s.log.Warn("fetching etcd members failed, sleeping", "tried", endpoints, "sleep", sleep)

			select {
			case <-time.After(sleep):
			case errc = <-s.quit:
			}
			continue
		}
		trials = 0

		// Filter on the local network interface
		local := []*net.IPAddr{}
		for address, _ := range peers {
			// Resolve the IP address from the host string
			addr, fail := net.ResolveIPAddr("ip", address)
			if fail != nil {
				err = fail
				break
			}
			// Filter out non-owned IPs
			if s.ipnet.Contains(addr.IP) {
				local = append(local, addr)
			}
		}
		if err != nil {
			continue
		}
		// Send the peers upstream and wait
		s.log.Info("reporting seed list", "seeds", local)
		for _, addr := range local {
			select {
			case sink <- addr:
			case errc = <-s.quit:
			}
		}
		// Wait until closure or the next cycle
		var rescan <-chan time.Time
		if atomic.LoadUint32(phase) == 0 {
			rescan = time.After(config.BootCoreOSFastRescan)
		} else {
			rescan = time.After(config.BootCoreOSSlowRescan)
		}
		select {
		case errc = <-s.quit:
		case <-rescan:
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
