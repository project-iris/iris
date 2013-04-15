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
package overlay

import (
	"config"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
	"net"
	"proto/bootstrap"
	"proto/session"
	"time"
)

// A source-tagged bootstrap peer address.
type tagBoot struct {
	tag  *net.TCPAddr
	addr *net.TCPAddr
}

// A source-tagged pastry session.
type tagSes struct {
	tag  *net.TCPAddr
	conn *session.Session
}

// Starts up the overlay networking on a specified interface and fans in all the
// inbound connections into the overlay-global channels, tagging them with the
// source interface.
func (o *overlay) acceptor(ip net.IP) {
	// Listen for incomming session on the given interface and random port.
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(ip.String(), "0"))
	if err != nil {
		panic(fmt.Sprintf("failed to resolve interface (%v): %v.", ip, err))
	}
	sesSink, quit, err := session.Listen(addr, o.lkey, o.rkeys)
	if err != nil {
		panic(fmt.Sprintf("failed to start session listener: %v.", err))
	}
	defer close(quit)

	// Start the bootstrapper on the specified interface
	bootSink, quit, err := bootstrap.Boot(ip, []byte(o.self), addr.Port)
	if err != nil {
		panic(fmt.Sprintf("failed to start bootstrapper: %v.", err))
	}
	defer close(quit)

	// Loop indefinitely, faning in the sessions and discovered peers
	for {
		select {
		case <-o.quit:
			return
		case boot := <-bootSink:
			o.bootSink <- &tagBoot{addr, boot}
		case ses := <-sesSink:
			o.sesSink <- &tagSes{addr, ses}
		}
	}
}

// Filters out inbound connections, and executes the handshake for those that
// are deemed useful.
func (o *overlay) shaker() {
	for {
		select {
		case <-o.quit:
			return
		case boot := <-o.bootSink:
			// Connect'em all (for now) !!!
			fmt.Println("Bootstrap found peer:", boot)
			go func() {
				if ses, err := session.Dial(boot.addr.IP.String(), boot.addr.Port, o.self, o.lkey, o.rkeys[o.self]); err != nil {
					log.Printf("failed to dial remote pastry peer: %v.", err)
				} else {
					o.sesSink <- &tagSes{boot.tag, ses}
				}
			}()
		case ses := <-o.sesSink:
			// Wait for peer init packet for real address
			fmt.Println("Pastry session connected:", ses)
			go o.shake(ses)
		case peer := <-o.peerSink:
			fmt.Println("Woot, initialized connection:", peer.addr)
			// Integrate peer into routing table and start routing
			o.state.mutex.Lock()
			// Do black magic
			o.state.mutex.Unlock()
			go o.receiver(peer)
		}
	}
}

// Pastry handshake to sort out the real hosts and ports.
func (o *overlay) shake(ses *tagSes) {
	// Create a new peer structure to hold all the needed session data
	p := new(peer)
	p.dec = gob.NewDecoder(&p.inBuf)
	p.enc = gob.NewEncoder(&p.outBuf)
	p.quit = make(chan struct{})
	p.in = make(chan *session.Message)
	p.out = ses.conn.Communicate(p.in, p.quit)

	// Send an init packet to the remote peer
	pkt := new(initPacket)
	pkt.Addr = ses.tag.String()
	if err := p.enc.Encode(pkt); err != nil {
		log.Printf("failed to encode init packet: %v.", err)
		return
	}
	msg := new(session.Message)
	msg.Head.Meta = make([]byte, p.outBuf.Len())
	copy(msg.Head.Meta, p.outBuf.Bytes())
	p.out <- msg

	// Wait for an incoming init packet
	timeout := time.Tick(time.Duration(config.PastryInitTimeout) * time.Millisecond)
	select {
	case <-timeout:
		log.Printf("session initialization timed out: %vms.", config.PastryInitTimeout)
		return
	case msg, ok := <-p.in:
		if !ok {
			log.Printf("remote closed connection before init packet.")
			return
		}
		p.inBuf.Write(msg.Head.Meta)
		if err := p.dec.Decode(pkt); err != nil {
			log.Printf("failed to decode remote init packet: %v.", err)
			return
		}
		hash := config.PastryHash.New()
		hash.Write([]byte(pkt.Addr))

		p.addr = pkt.Addr
		p.id = big.NewInt(0)
		p.id.SetBytes(hash.Sum(nil))

		// Evenrything ok, accept connection
		o.peerSink <- p
	}
}
