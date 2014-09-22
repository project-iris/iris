// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

// Package bootstrap is responsible for randomly probing and linearly scanning
// the local network (single interface) for other running instances.
//
// In every scanning cycle all configured UDP ports are checked (to prevent
// slowdowns due to large config space).
//
// Since the heartbeats are on UDP, each one is flagged as a beat request or
// response (i.e. reply to requests, but don't loop indefinitely).
package bootstrap

import (
	"bytes"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/gobber"
)

// Constants for the protocol UDP layer
var acceptTimeout = 250 * time.Millisecond

// A direction tagged (req/resp) bootstrap event.
type Event struct {
	Peer *big.Int     // Overlay node id of the peer
	Addr *net.TCPAddr // TCP address of the peer
	Resp bool         // Flag specifying bootstrap event type
}

// Bootstrap state message.
type Message struct {
	Version string
	Magic   []byte
	NodeId  *big.Int
	Overlay int
	Request bool
}

// Bootstrapper state for a single network interface.
type Bootstrapper struct {
	addr *net.UDPAddr
	sock *net.UDPConn
	mask *net.IPMask

	magic    []byte // Filters side-by-side Iris networks
	request  []byte // Pre-generated request packet
	response []byte // Pre-generated response packet

	gob *gobber.Gobber // Datagram gobber to decode the network messages

	beats chan *Event     // Channel on which to report bootstrap events
	quit  chan chan error // Quit channel to synchronize bootstrapper termination

	fast bool
}

// Creates a new bootstrapper, configuring to listen on the given interface for
// for incoming requests and scan the same interface for other peers. The magic
// is used to filter multiple Iris networks in the same physical network, while
// the overlay is the TCP listener port of the DHT.
func New(ipnet *net.IPNet, magic []byte, node *big.Int, overlay int) (*Bootstrapper, chan *Event, error) {
	// Issue a warning if we have a single machine subnet
	if ones, bits := ipnet.Mask.Size(); bits-ones < 2 {
		log.Printf("bootstrap: WARNING! cannot search on interface %v, network mask covers the whole space", ipnet)
	}
	bs := &Bootstrapper{
		magic: magic,
		beats: make(chan *Event, config.BootBeatsBuffer),
		fast:  true,
	}
	// Open the server socket
	var err error
	for _, port := range config.BootPorts {
		bs.addr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(ipnet.IP.String(), strconv.Itoa(port)))
		if err != nil {
			return nil, nil, err
		}
		bs.sock, err = net.ListenUDP("udp", bs.addr)
		if err != nil {
			continue
		} else {
			bs.addr.Port = bs.sock.LocalAddr().(*net.UDPAddr).Port
			bs.mask = &ipnet.Mask
			break
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("no available ports")
	}
	// Generate the local heartbeat messages (request and response)
	bs.magic = magic
	bs.gob = gobber.New()
	bs.gob.Init(new(Message))

	msg := Message{
		Version: config.ProtocolVersion,
		Magic:   magic,
		NodeId:  node,
		Overlay: overlay,
		Request: true,
	}
	if buf, err := bs.gob.Encode(msg); err != nil {
		return nil, nil, fmt.Errorf("request encode failed: %v.", err)
	} else {
		bs.request = make([]byte, len(buf))
		copy(bs.request, buf)
	}

	msg.Request = false
	if buf, err := bs.gob.Encode(msg); err != nil {
		return nil, nil, fmt.Errorf("response encode failed: %v.", err)
	} else {
		bs.response = make([]byte, len(buf))
		copy(bs.response, buf)
	}
	// Return the ready-to-boot bootstrapper
	return bs, bs.beats, nil
}

// Starts accepting bootstrap events and initiates peer discovery.
func (bs *Bootstrapper) Boot() error {
	bs.quit = make(chan chan error, 3)

	go bs.accept()
	go bs.probe()
	go bs.scan()

	return nil
}

// Closes the bootstrap listener and terminates all probing procedures.
func (bs *Bootstrapper) Terminate() error {
	// Make sure the bootstrapper was actually started
	if bs.quit == nil {
		return fmt.Errorf("non-booted bootstrapper")
	}
	// Retrieve three errors for the acceptor, prober and scanner routines
	errc := make([]chan error, 3)
	errs := []error{}
	for i := 0; i < len(errc); i++ {
		errc[i] = make(chan error, 1)
		bs.quit <- errc[i]
	}
	for i := 0; i < len(errc); i++ {
		if err := <-errc[i]; err != nil {
			errs = append(errs, err)
		}
	}
	// Report the errors and return
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return fmt.Errorf("%v", errs)
	}
}

// Switches between startup (fast) and maintenance (slow) sampling speeds.
func (bs *Bootstrapper) SetMode(startup bool) {
	bs.fast = startup
}

// Heartbeat and connect packet acceptor routine. It listens for incoming UDP
// packets, and for each one verifies that the protocol version and bootstrap
// magic number match the local one. If the verifications passes, the remote
// overlay's id and listener port is sent to the maintenance thread to sort out.
func (bs *Bootstrapper) accept() {
	buf := make([]byte, 1500) // UDP MTU
	var errc chan error

	// Repeat the packet processing until termination is requested
	for errc == nil {
		select {
		case errc = <-bs.quit:
			break
		default:
			// Wait for a UDP packet (with a reasonable timeout)
			bs.sock.SetReadDeadline(time.Now().Add(acceptTimeout))
			if size, from, err := bs.sock.ReadFromUDP(buf); err == nil {
				msg := new(Message)
				if err := bs.gob.Decode(buf[:size], msg); err == nil {
					if config.ProtocolVersion == msg.Version && msg.Magic != nil && bytes.Compare(bs.magic, msg.Magic) == 0 {
						// If it's a beat request, respond to it
						if msg.Request {
							bs.sock.WriteToUDP(bs.response, from)
						}
						// Notify the maintenance routine
						host := net.JoinHostPort(from.IP.String(), strconv.Itoa(msg.Overlay))
						if addr, err := net.ResolveTCPAddr("tcp", host); err == nil {
							bs.beats <- &Event{
								Peer: msg.NodeId,
								Addr: addr,
								Resp: !msg.Request,
							}
						}
					}
				}
			}
		}
	}
	// Clean up resources and report results
	close(bs.beats)
	errc <- bs.sock.Close()
}

// Sends heartbeat messages to random hosts on the listener-local address. The
// IP addresses are generated uniformly inside the subnet and all ports in the
// config array are tried simultaneously. Self connection is disabled.
func (bs *Bootstrapper) probe() {
	// Set up some initial parameters
	ones, bits := bs.mask.Size()

	// Short circuit if malformed network (i.e. no space to probe)
	if bits-ones < 2 {
		errc := <-bs.quit
		errc <- nil
		return
	}
	// Probe random addresses until termination is requested
	var errc chan error
	for errc == nil {
		select {
		case errc = <-bs.quit:
			break
		default:
			// Generate a random IP address within the subnet (ignore net and bcast)
			host := bs.addr.IP
			for host.Equal(bs.addr.IP) {
				subip := rand.Intn(1<<uint(bits-ones)-2) + 1
				host = bs.addr.IP.Mask(*bs.mask)
				for i := len(host) - 1; i >= 0; i-- {
					host[i] |= byte(subip & 255)
					subip >>= 8
				}
			}
			// Iterate over every bootstrap port
			for _, port := range config.BootPorts {
				dest := net.JoinHostPort(host.String(), strconv.Itoa(port))

				// Resolve the address, connect to it and send a beat request
				raddr, err := net.ResolveUDPAddr("udp", dest)
				if err != nil {
					panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", dest, err))
				}
				bs.sock.WriteToUDP(bs.request, raddr)
			}
			// Wait for the next cycle
			var wake <-chan time.Time
			if bs.fast {
				wake = time.After(time.Duration(config.BootFastProbe) * time.Millisecond)
			} else {
				wake = time.After(time.Duration(config.BootSlowProbe) * time.Millisecond)
			}
			select {
			case errc = <-bs.quit:
			case <-wake:
			}
		}
	}
	// Report termination
	errc <- nil
}

// Scans the network linearly from the current address, sending heartbeat
// messages. Self connection is disabled.
func (bs *Bootstrapper) scan() {
	// Set up some initial parameters
	size := len(bs.addr.IP)
	ones, bits := bs.mask.Size()
	subip := 0
	for i := 0; i < bits-ones; i++ {
		subip += int(bs.addr.IP[size-1-i/8]) & (1 << uint(i%8))
	}

	offset := 0
	downDone, upDone := false, false

	// Scan until either the whole space is covered or termination is requested
	var errc chan error
	var done bool
	for done == false && errc == nil {
		select {
		case errc = <-bs.quit:
			break
		default:
			// Quit if both directions have been scanned
			if downDone && upDone {
				done = true
				break
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
			host := bs.addr.IP.Mask(*bs.mask)
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

				// Resolve the address, connect to it and send a beat request
				raddr, err := net.ResolveUDPAddr("udp", dest)
				if err != nil {
					panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", dest, err))
				}
				bs.sock.WriteToUDP(bs.request, raddr)
			}
			// Wait for the next cycle
			select {
			case errc = <-bs.quit:
			case <-time.After(time.Duration(config.BootScan) * time.Millisecond):
			}
		}
	}
	// Wait for the termination request if needed and report
	if errc == nil {
		errc = <-bs.quit
	}
	errc <- nil
}
