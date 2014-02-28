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

package overlay

import (
	"fmt"
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

	// Virtual id and reachable addresses
	nodeId *big.Int
	addrs  []string

	// Connection details
	conn *session.Session

	laddr string // Local address, flattened
	raddr string // Remote address, flattened
	lhost string // Local IP, flattened
	rhost string // Remote IP, flattened

	// Overlay state infos
	time    uint64
	passive bool

	// Maintenance fields
	init bool            // Specifies whether the receiver was started
	quit chan chan error // Quit channe to synchronize peer termination
	term chan struct{}   // Termination signaller to prevent new operations

	quitLock sync.Mutex // Protect concurrent closes
}

// Creates a new peer structure, ready to begin communicating.
func (o *Overlay) newPeer(ses *session.Session) (*peer, error) {
	ses.Start(config.OverlayNetBuffer)

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
		term: make(chan struct{}),
	}, nil
}

// Starts the inbound packet acceptor for the peer connection.
func (p *peer) Start() error {
	// Make sure we haven't been already terminated
	p.quitLock.Lock()
	defer p.quitLock.Unlock()

	if p.quit == nil {
		return fmt.Errorf("closed")
	}
	// Sanity check to catch programming bugs
	if p.init {
		panic("overlay: peer connection already started")
	}
	// Otherwise start the overlay receiver and return success
	p.init = true
	go p.receiver()
	return nil
}

// Terminates a peer connection and sets the quit channel to nil to prevent
// double close.
func (p *peer) Close() error {
	// Sync between concurrent closes
	p.quitLock.Lock()
	defer p.quitLock.Unlock()

	// If the link was never up, close and be happy
	if !p.init {
		p.quit = nil
		close(p.term)
		return p.conn.Close()
	}
	// Otherwise do a synced close
	errc := make(chan error)
	select {
	case p.quit <- errc:
		// Clear the quit channel to ensure single close, return result
		p.quit = nil
		return <-errc
	default:
		// Already closed, return success
		return nil
	}
}

// Sends a message to the remote peer.
func (p *peer) send(msg *proto.Message) error {
	// Send the message or time out
	select {
	case <-p.term:
		return fmt.Errorf("closed")
	case <-time.After(time.Duration(config.OverlaySendTimeout) * time.Millisecond):
		return fmt.Errorf("timeout")
	case p.conn.CtrlLink.Send <- msg:
		return nil
	}
}

// Listens for inbound messages from the peer and routes them into the overlay
// network.
func (p *peer) receiver() {
	var errc chan error
	var errv error

	// Retrieve messages until termination is requested or the connection fails
	for closed := false; !closed && errc == nil; {
		select {
		case <-p.owner.quit:
			// TODO: Fix this up properly, HACK HACK HACK
			closed = true
			break
		case errc = <-p.quit:
			//go func() { p.owner.dropSink <- p }()
			break
		case msg, ok := <-p.conn.CtrlLink.Recv:
			// Signal the owning overlay in case of a remote error
			if !ok {
				// TODO: Is this go routine really necessary?
				// TODO: Sync this up with overlay close logic!
				go func() { p.owner.dropSink <- p }()
				closed = true
				break
			}
			// Check whether it's a close request
			p.owner.route(p, msg)
		}
	}
	// Signal to all that the link is closed
	close(p.term)

	// Terminate the session
	if err := p.conn.Close(); errv == nil {
		errv = err
	}
	// Report termination result
	if errc == nil {
		errc = <-p.quit
	}
	errc <- errv
}
