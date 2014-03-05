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

// Contains the peer connection state information and the related maintenance
// operations.

package pastry

import (
	"errors"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/session"
)

// Peer state information.
type peer struct {
	owner *Overlay
	conn  *session.Session

	// Virtual id and reachable addresses
	nodeId *big.Int
	addrs  []string

	// Connection details
	laddr string // Local address, flattened
	raddr string // Remote address, flattened
	lhost string // Local IP, flattened
	rhost string // Remote IP, flattened

	// Overlay state infos
	time    uint64
	passive bool

	// Maintenance fields
	quit chan chan error // Synchronizes peer termination
	term bool            // Specifies whether the peer terminated already or not
	lock sync.Mutex      // Lock to protect the close mechanism
}

// Creates a new peer instance, ready to begin communicating.
func (o *Overlay) newPeer(ses *session.Session) *peer {
	return &peer{
		owner: o,
		conn:  ses,

		// Connection details
		laddr: ses.CtrlLink.Sock().LocalAddr().String(),
		raddr: ses.CtrlLink.Sock().RemoteAddr().String(),
		lhost: ses.CtrlLink.Sock().LocalAddr().(*net.TCPAddr).IP.String(),
		rhost: ses.CtrlLink.Sock().LocalAddr().(*net.TCPAddr).IP.String(),

		// Transport and maintenance channels
		quit: make(chan chan error),
	}
}

// Starts the inbound message processor and router.
func (p *peer) Start() {
	go p.processor()
}

// Terminates a peer connection.
func (p *peer) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// If the peer was already closed, return
	if p.term {
		return nil
	}
	p.term = true

	// Gracefully close the peer session, flushing pending messages
	res := p.conn.Close()

	// Sync the processor termination and return
	errc := make(chan error)
	p.quit <- errc
	if err := <-errc; res != nil {
		res = err
	}
	return res
}

// Sends a message to the remote peer.
func (p *peer) send(msg *proto.Message) error {
	// Select the outbound channel based on message contents
	link := p.conn.DataLink
	if len(msg.Data) == 0 {
		link = p.conn.CtrlLink
	}
	// Send the message on the selected channel
	select {
	case link.Send <- msg:
		return nil
	case <-time.After(config.OverlaySendTimeout):
		return errors.New("timeout")
	}
}

// Accepts inbound messages and routes them into the overlay.
func (p *peer) processor() {
	var errc chan error
	var errv error

	// Retrieve messages until connection is torn down or termination is requested
	// Note, channels are prioritized, with control first and data second
	for closed := false; !closed && errc == nil; {
		select {
		case errc = <-p.quit:
			// This should not happen often (graceful close on the recv channel instead)
			continue
		case msg, ok := <-p.conn.CtrlLink.Recv:
			if !ok {
				// Connection went down (gracefully or not, dunno)
				closed = true
				continue
			}
			// Route the control message
			p.owner.route(p, msg)
		default:
			// No control events, allow data events too
			select {
			case errc = <-p.quit:
				// This should not happen often (graceful close on the recv channel instead)
				continue
			case msg, ok := <-p.conn.CtrlLink.Recv:
				if !ok {
					// Connection went down (gracefully or not, dunno)
					closed = true
					continue
				}
				// Route the control message
				p.owner.route(p, msg)
			case msg, ok := <-p.conn.DataLink.Recv:
				if !ok {
					// Connection went down (gracefully or not, dunno)
					closed = true
					continue
				}
				// Route the data message
				p.owner.route(p, msg)
			}
		}
	}
	// Signal the overlay of the connection drop
	if errc == nil {
		p.owner.drop(p)
	}
	// Report termination result
	if errc == nil {
		errc = <-p.quit
	}
	errc <- errv
}
