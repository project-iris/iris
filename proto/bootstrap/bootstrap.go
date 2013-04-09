// Iris - Distributed Messaging Framework
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

// Package bootstrap is responsible for randomly probing and linearly scanning
// the local network (single interface) for other running instances. In every
// scanning cycle all configured UDP ports are checked (to prevent slowdowns due
// to large config space).
package bootstrap

import (
	"bytes"
	"config"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"time"
)

// Constants for the protocol UDP layer
var acceptTimeout = time.Second

// Bootstrapper state for a single network interface.
type bootstrapper struct {
	addr  *net.UDPAddr
	daddr *net.UDPAddr
	sock  *net.UDPConn

	beat []byte

	beats chan *net.TCPAddr
	quit  chan struct{}

	fast bool
}

// Starts up the bootstrapping module. Bootstrapping will listen on the given
// interface and available port (out of the allowed ones) for incomming requests,
// and will scan that specific interface for other peers. The overlay argument
// is used to advertize the overlay TCP listener.
func Boot(ip net.IP, overlay int) (chan *net.TCPAddr, chan struct{}, error) {
	bs := new(bootstrapper)
	bs.fast = true

	// Open the server socket
	var err error
	for _, port := range config.BootPorts {
		bs.addr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(ip.String(), strconv.Itoa(port)))
		if err != nil {
			return nil, nil, err
		}
		bs.sock, err = net.ListenUDP("udp", bs.addr)
		if err != nil {
			continue
		} else {
			bs.addr.Port = bs.sock.LocalAddr().(*net.UDPAddr).Port
			break
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to listen on all configured ports: %v.", err)
	}
	bs.daddr = new(net.UDPAddr)
	*bs.daddr = *(bs.addr)
	bs.daddr.Port = 0

	// Save the magic number and generate the local heartbeat message
	bs.beat = make([]byte, len(config.BootMagic)+3)
	copy(bs.beat, config.BootMagic)
	binary.PutUvarint(bs.beat[len(config.BootMagic):], uint64(overlay))

	// Create the channels for the communicating threads
	bs.beats = make(chan *net.TCPAddr, config.BootBeatsBuffer)
	bs.quit = make(chan struct{})

	// Create the two channels, start the packet acceptor and return
	go bs.accept()
	go bs.probe()
	go bs.scan()

	return bs.beats, bs.quit, nil
}

// Switches between startup (fast) and maintenance (slow) sampling speeds.
func (bs *bootstrapper) SetMode(startup bool) {
	bs.fast = startup
}

// Heartbeat and connect packet acceptor routine. It listens for incomming UDP
// packets, and for each one verifies that the bootstrap magic number matches
// the local one. If the verifications passed, the TCP port within is extracted
// an is sent to the maintenance thread to sort out.
func (bs *bootstrapper) accept() {
	defer close(bs.beats)
	defer bs.sock.Close()

	buf := make([]byte, 1500) // UDP MTU
	for {
		select {
		case <-bs.quit:
			return
		default:
			// Wait for a UDP packet (with a reasonable timeout)
			bs.sock.SetReadDeadline(time.Now().Add(acceptTimeout))
			_, from, err := bs.sock.ReadFromUDP(buf)
			if err == nil && bytes.Compare(config.BootMagic, buf[:len(config.BootMagic)]) == 0 {
				// Extract the IP address and notify the maintenance routine
				if port, err := binary.ReadUvarint(bytes.NewBuffer(buf[len(config.BootMagic):])); err == nil {
					host := net.JoinHostPort(from.IP.String(), strconv.Itoa(int(port)))
					if addr, err := net.ResolveTCPAddr("tcp", host); err == nil {
						bs.beats <- addr
					}
				}
			}
		}
	}
}

// Sends heartbeat messages to random hosts on the listener-local address. The
// IP addresses are generated uniformly inside the subnet and all ports in the
// config array are tried simultaneously. Self connection is disabled.
func (bs *bootstrapper) probe() {
	// Set up some initial parameters
	mask := bs.addr.IP.DefaultMask()
	ones, bits := mask.Size()

	for {
		select {
		case <-bs.quit:
			return
		default:
			// Generate a random IP address within the subnet (ignore net and bcast)
			host := bs.addr.IP
			for host.Equal(bs.addr.IP) {
				subip := rand.Intn(1<<uint(bits-ones)-2) + 1
				host = bs.addr.IP.Mask(mask)
				for i := len(host) - 1; i >= 0; i-- {
					host[i] |= byte(subip & 255)
					subip >>= 8
				}
			}
			// Iterate over every bootstrap port
			for _, port := range config.BootPorts {
				dest := net.JoinHostPort(host.String(), strconv.Itoa(port))

				// Resolve the address, connect to it and send a beat
				raddr, err := net.ResolveUDPAddr("udp", dest)
				if err != nil {
					panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", dest, err))
				}
				sock, err := net.DialUDP("udp", bs.daddr, raddr)
				if err != nil {
					panic(fmt.Sprintf("failed to dial remote bootstrapper (%v): %v.", raddr, err))
				} else {
					sock.Write(bs.beat)
					sock.Close()
				}
				// Wait for the next cycle
				if bs.fast {
					time.Sleep(time.Duration(config.BootFastProbe) * time.Millisecond)
				} else {
					time.Sleep(time.Duration(config.BootSlowProbe) * time.Millisecond)
				}
			}
		}
	}
}

// Scans the network linearly from the current address, sending heartbeat
// messages. Self connection is disabled.
func (bs *bootstrapper) scan() {
	// Set up some initial parameters
	mask := bs.addr.IP.DefaultMask()
	ones, bits := mask.Size()
	subip := 0
	for i := 0; i < bits-ones; i++ {
		subip += int(bs.addr.IP[(bits-1-i)/8]) & (1 << uint(i%8))
	}

	offset := 0
	downDone, upDone := false, false
	for {
		select {
		case <-bs.quit:
			return
		default:
			// Quit if both directions have been scanned
			if downDone && upDone {
				return
			}
			// Generate the next neighboring IP address
			scanip := subip + offset
			if offset <= 0 {
				offset = -offset + 1
			} else {
				offset = -offset
			}
			// Make sure we didn't run out of the subnet
			if scanip <= 0 {
				downDone = true
				continue
			} else if scanip >= (1<<uint(bits-ones))-1 {
				upDone = true
				continue
			}
			// Generate the new address
			host := bs.addr.IP.Mask(mask)
			for i := len(host) - 1; i >= 0; i-- {
				host[i] |= byte(scanip & 255)
				scanip >>= 8
			}
			// Iterate over every bootstrap port
			for _, port := range config.BootPorts {
				// Don't connect to ourselves
				if port == bs.addr.Port && host.Equal(bs.addr.IP) {
					continue
				}
				dest := net.JoinHostPort(host.String(), strconv.Itoa(port))

				// Resolve the address, connect to it and send a beat
				raddr, err := net.ResolveUDPAddr("udp", dest)
				if err != nil {
					panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", dest, err))
				}
				sock, err := net.DialUDP("udp", bs.daddr, raddr)
				if err != nil {
					panic(fmt.Sprintf("failed to dial remote bootstrapper (%v): %v.", raddr, err))
				} else {
					sock.Write(bs.beat)
					sock.Close()
				}
			}
			// Wait for the next cycle
			time.Sleep(time.Duration(config.BootScan) * time.Millisecond)
		}
	}
}
