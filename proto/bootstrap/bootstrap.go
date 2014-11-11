// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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
	"math/big"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/gobber"
	"gopkg.in/inconshreveable/log15.v2"
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
	Magic    []byte   // Magic blob ensuring an Iris bootstrapper
	Version  string   // Version string to prevent protocol mismatches
	Owner    *big.Int // Numerical identifier of bootstrapper owner
	Endpoint int      // TCP listener port of the owner process
	Request  bool     // Flag to decide whether to respond or not
}

// Interface for seed generator algorithms.
type seeder interface {
	// Starts the seed generator. Suggested peers are reported through the sink
	// channel, while the phase argument is used to switch between booting and
	// convergence phases (0/1 values used only).
	Start(sink chan *net.IPAddr, phase *uint32) error

	// Terminates the seed generator, retuning any errors that occurred.
	Close() error
}

// Bootstrapper state for a single network interface.
type Bootstrapper struct {
	ipnet *net.IPNet
	addr  *net.UDPAddr
	sock  *net.UDPConn
	mask  *net.IPMask

	magic    []byte // Filters side-by-side Iris networks
	request  []byte // Pre-generated request packet
	response []byte // Pre-generated response packet

	// Seeding algorithms and address sink fields
	scanSeed, probeSeed, coreOSSeed seeder
	scanSink, probeSink, coreOSSink chan *net.IPAddr

	gob *gobber.Gobber // Datagram gobber to decode the network messages

	beats chan *Event // Channel on which to report bootstrap events
	phase uint32      // Phase of the bootstrapper (0 = fase, 1 = slow)

	// Maintenance fields
	quit chan chan error // Quit channel to synchronize bootstrapper termination
	log  log15.Logger    // Contextual logger with package name injected
}

// Creates a new bootstrapper, configuring to listen on the given interface for
// for incoming requests and scan the same interface for other peers. The magic
// is used to filter multiple Iris networks in the same physical network, while
// the overlay is the TCP listener port of the DHT.
func New(ipnet *net.IPNet, magic []byte, owner *big.Int, endpoint int) (*Bootstrapper, chan *Event, error) {
	logger := log15.New("subsys", "bootstrap", "ipnet", ipnet)
	gobber := gobber.New()
	gobber.Init(new(Message))

	// Do some environment dependent IP black magic
	if detectGoogleComputeEngine() {
		// Google Compute Engine issues /32 addresses, find real subnet
		logger.Info("detected GCE, retrieving real netmask")
		old := ipnet.String()
		if err := updateIPNet(ipnet); err != nil {
			logger.Error("failed to retrieve netmask, defaulting", "error", err)
			ipnet.Mask = ipnet.IP.DefaultMask()
		}
		logger.Info("network subnet mask replaced", "old", old, "new", ipnet)
		logger = log15.New("subsys", "bootstrap", "ipnet", ipnet)
	}
	b := &Bootstrapper{
		ipnet: ipnet,
		magic: magic,
		beats: make(chan *Event, config.BootBeatsBuffer),
		phase: uint32(0),

		gob:      gobber,
		request:  newBootstrapRequest(magic, owner, endpoint, gobber),
		response: newBootstrapResponse(magic, owner, endpoint, gobber),

		// Seeding algorithms and address sinks
		scanSeed:   newScanSeeder(ipnet, logger),
		probeSeed:  newProbeSeeder(ipnet, logger),
		coreOSSeed: newCoreOSSeeder(ipnet, logger),
		scanSink:   make(chan *net.IPAddr, config.BootSeedSinkBuffer),
		probeSink:  make(chan *net.IPAddr, config.BootSeedSinkBuffer),
		coreOSSink: make(chan *net.IPAddr, config.BootSeedSinkBuffer),

		// Maintenance fields
		quit: make(chan chan error),
		log:  logger,
	}
	// Open the server socket
	var err error
	for _, port := range config.BootPorts {
		b.addr, err = net.ResolveUDPAddr("udp", net.JoinHostPort(ipnet.IP.String(), strconv.Itoa(port)))
		if err != nil {
			return nil, nil, err
		}
		b.sock, err = net.ListenUDP("udp", b.addr)
		if err != nil {
			continue
		} else {
			b.addr.Port = b.sock.LocalAddr().(*net.UDPAddr).Port
			b.mask = &ipnet.Mask
			break
		}
	}
	if err != nil {
		return nil, nil, fmt.Errorf("no available ports")
	}
	// Return the ready-to-boot bootstrapper
	return b, b.beats, nil
}

// Creates a bootstrap request message to send to potential peers.
func newBootstrapRequest(magic []byte, owner *big.Int, endpoint int, encoder *gobber.Gobber) []byte {
	msg := newBootstrapMessage(magic, owner, endpoint)
	msg.Request = true

	if buf, err := encoder.Encode(msg); err != nil {
		panic(err)
	} else {
		request := make([]byte, len(buf))
		copy(request, buf)
		return request
	}
}

// Creates a bootstrap response message to send to querying peers.
func newBootstrapResponse(magic []byte, owner *big.Int, endpoint int, encoder *gobber.Gobber) []byte {
	msg := newBootstrapMessage(magic, owner, endpoint)
	msg.Request = false

	if buf, err := encoder.Encode(msg); err != nil {
		panic(err)
	} else {
		response := make([]byte, len(buf))
		copy(response, buf)
		return response
	}
}

// Creates a generic bootstrap message to send between nodes.
func newBootstrapMessage(magic []byte, owner *big.Int, endpoint int) *Message {
	return &Message{
		Magic:    magic,
		Version:  config.ProtocolVersion,
		Owner:    owner,
		Endpoint: endpoint,
	}
}

// Starts accepting bootstrap events and initiates peer discovery.
func (b *Bootstrapper) Boot() error {
	// Start all of the seeding algorithms
	if err := b.scanSeed.Start(b.scanSink, &b.phase); err != nil {
		return err
	}
	if err := b.probeSeed.Start(b.probeSink, &b.phase); err != nil {
		b.scanSeed.Close()
		return err
	}
	if err := b.coreOSSeed.Start(b.coreOSSink, &b.phase); err != nil {
		b.probeSeed.Close()
		b.scanSeed.Close()
		return err
	}
	// Start the bootstrap message initiator and acceptor
	go b.initiator()
	go b.acceptor()

	return nil
}

// Closes the bootstrap listener and terminates all probing procedures.
func (b *Bootstrapper) Terminate() error {
	// Make sure the bootstrapper was actually started
	if b.quit == nil {
		return fmt.Errorf("non-booted bootstrapper")
	}
	// Retrieve errors for the initiator and acceptor
	errc := make([]chan error, 2)
	errs := []error{}
	for i := 0; i < len(errc); i++ {
		errc[i] = make(chan error, 1)
		b.quit <- errc[i]
	}
	for i := 0; i < len(errc); i++ {
		if err := <-errc[i]; err != nil {
			errs = append(errs, err)
		}
	}
	// Terminate the seeding algorithms
	if err := b.coreOSSeed.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := b.probeSeed.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := b.scanSeed.Close(); err != nil {
		errs = append(errs, err)
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
func (b *Bootstrapper) SetMode(startup bool) {
	if startup {
		atomic.StoreUint32(&b.phase, uint32(0))
	} else {
		atomic.StoreUint32(&b.phase, uint32(1))
	}
}

// Bootstrap message initiator retrieving possible peer addresses from various
// seed generators and sending bootstrap requests at a given rate.
func (b *Bootstrapper) initiator() {
	b.log.Info("starting initiator")

	// Repeat message initiation until termination is requested
	var errc chan error
	for errc == nil {
		// Retrieve a possible seed address from a random algorithm
		var addr *net.IPAddr
		select {
		// Short circuit termination request
		case errc = <-b.quit:
			continue

		default:
			select {
			case addr = <-b.scanSink:
			case addr = <-b.probeSink:
			case addr = <-b.coreOSSink:
			case errc = <-b.quit:
				continue
			}
		}
		// Discard self addresses
		self := 0
		if b.ipnet.IP.String() == addr.IP.String() {
			self = b.addr.Port
		}
		// Send a bootstrap request on all configured ports
		for _, port := range config.BootPorts {
			if port == self {
				continue
			}
			host := net.JoinHostPort(addr.String(), strconv.Itoa(port))

			addr, err := net.ResolveUDPAddr("udp", host)
			if err != nil {
				panic(fmt.Sprintf("failed to resolve remote bootstrapper (%v): %v.", host, err))
			}
			b.sock.WriteToUDP(b.request, addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Report termination and sync closer
	b.log.Info("terminating initiator")
	errc <- nil
}

// Heartbeat and connect packet acceptor routine. It listens for incoming UDP
// packets, and for each one verifies that the protocol version and bootstrap
// magic number match the local one. If the verifications passes, the remote
// overlay's id and listener port is sent to the maintenance thread to sort out.
func (b *Bootstrapper) acceptor() {
	b.log.Info("starting acceptor")
	buf := make([]byte, 1500) // UDP MTU

	// Repeat the packet processing until termination is requested
	var errc chan error
	for errc == nil {
		select {
		case errc = <-b.quit:
			continue
		default:
			// Wait for a UDP packet (with a reasonable timeout)
			b.sock.SetReadDeadline(time.Now().Add(acceptTimeout))
			if size, from, err := b.sock.ReadFromUDP(buf); err == nil {
				msg := new(Message)
				if err := b.gob.Decode(buf[:size], msg); err == nil {
					if config.ProtocolVersion == msg.Version && msg.Magic != nil && bytes.Compare(b.magic, msg.Magic) == 0 {
						// If it's a beat request, respond to it
						if msg.Request {
							b.sock.WriteToUDP(b.response, from)
						}
						// Notify the maintenance routine
						host := net.JoinHostPort(from.IP.String(), strconv.Itoa(msg.Endpoint))
						if addr, err := net.ResolveTCPAddr("tcp", host); err == nil {
							event := &Event{
								Peer: msg.Owner,
								Addr: addr,
								Resp: !msg.Request,
							}
							select {
							case b.beats <- event:
							case errc = <-b.quit:
								continue
							}
						}
					}
				}
			}
		}
	}
	b.log.Info("terminating acceptor")

	// Clean up resources and report results
	close(b.beats)
	errc <- b.sock.Close()
}
