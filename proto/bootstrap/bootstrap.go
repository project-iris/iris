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

type Action uint8

const (
	birth Action = iota
	death
)

type Event struct {
	action Action
	entity *net.TCPAddr
}

type bootstrapper struct {
	addr *net.UDPAddr
	sock *net.UDPConn

	beat []byte

	events chan *Event
	beats  chan *net.TCPAddr
	quit   chan struct{}
}

func Boot(addr *net.UDPAddr, port uint16) (chan *Event, chan struct{}, error) {
	bs := new(bootstrapper)
	bs.addr = addr

	// Open the server socket
	var err error
	bs.sock, err = net.ListenUDP("udp", addr)
	if err != nil {
		return nil, nil, err
	}
	addr.Port = bs.sock.LocalAddr().(*net.UDPAddr).Port

	// Save the magic number and generate the local heartbeat message
	bs.beat = make([]byte, len(config.BootMagic)+2)
	copy(bs.beat, config.BootMagic)
	binary.PutUvarint(bs.beat[len(config.BootMagic):], uint64(port))

	// Create the channels for the communicating threads
	bs.events = make(chan *Event)
	bs.beats = make(chan *net.TCPAddr)
	bs.quit = make(chan struct{})

	// Create the two channels, start the packet acceptor and return
	go bs.maintain()
	go bs.accept()
	go bs.probe()

	return bs.events, bs.quit, nil
}

// Heartbeat and connect packet acceptor routine. It listens for incomming UDP
// packets, and for each one verifies that the bootstrap magic number matches
// the local one. If the verifications passed, the TCP port within is extracted
// an is sent to the maintenance thread to sort out.
func (bs *bootstrapper) accept() {
	defer close(bs.beats)
	defer bs.sock.Close()

	buf := make([]byte, 1500)
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
// IP addresses are generated uniformly inside the subnet and the ports picked
// again at uniform random from the config array. Self connection is disabled.
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
			port := bs.addr.Port
			for bytes.Compare(host, bs.addr.IP) == 0 && port == bs.addr.Port {
				subip := rand.Intn(1<<uint(bits-ones)-2) + 1
				host := bs.addr.IP.Mask(mask)
				for i := len(host) - 1; i >= 0; i-- {
					host[i] |= byte(subip & 255)
					subip >>= 8
				}
				port = int(config.BootPorts[rand.Intn(len(config.BootPorts))])
			}
			dest := net.JoinHostPort(host.String(), strconv.Itoa(int(bs.addr.Port)))

			// Resolve the address, connect to it and send a beat
			raddr, err := net.ResolveUDPAddr("udp", dest)
			if err != nil {
				panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", dest, err))
			}
			sock, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				panic(fmt.Sprintf("failed to dial remote bootstrapper (%v): %v.", raddr, err))
			} else {
				sock.Write(bs.beat)
				sock.Close()
			}
		}
	}
}

func (bs *bootstrapper) maintain() {
	defer close(bs.events)

	for {
		select {
		case <-bs.quit:
			return
		case addr := <-bs.beats:
			fmt.Println("Beat: ", addr)
		}
	}
}
