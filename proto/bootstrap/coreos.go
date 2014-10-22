// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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
	ipnet *net.IPNet
	quit  chan chan error
	log   log15.Logger
}

// Creates a new CoreOS seed generator.
func newCoreOSSeeder(ipnet *net.IPNet, logger log15.Logger) (seeder, error) {
	return &coreOSSeeder{
		ipnet: ipnet,
		quit:  make(chan chan error),
		log:   logger.New("algo", "coreos"),
	}, nil
}

// Starts the seed generator.
func (s *coreOSSeeder) Start(sink chan *net.IPAddr, phase *uint32) error {
	go s.run(sink, phase)
	return nil
}

// Terminates the seed generator.
func (s *coreOSSeeder) Close() error {
	errc := make(chan error)
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
	trials := 0
	for err == nil && errc == nil {
		// Try and retrieve membership infos from all probable etcd endpoints
		errs, peers := []error{}, make(map[string]struct{})
		for _, port := range config.BootCoreOSPorts {
			for _, ip := range []string{"127.0.0.1", "[::1]"} {
				// Try and retrieve the member list from etcd
				res, err := http.Get(fmt.Sprintf("http://%s:%d/v2/admin/machines", ip, port))
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
		}
		// If no IPs have been found, allow a few trial, then error out
		if len(peers) == 0 {
			trials++
			if trials <= config.BootCoreOSRetries {
				sleep := time.Duration(trials) * config.BootCoreOSFailSleep
				s.log.Warn("fetching etcd membership failed", "sleep", sleep, "retries", config.BootCoreOSRetries-trials)
				select {
				case <-time.After(sleep):
				case errc = <-s.quit:
				}
			} else {
				err = fmt.Errorf("etcd failure: %v", errs)
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
