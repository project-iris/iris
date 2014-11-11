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

// Contains the peer connection state information and the related maintenance
// operations.

package pastry

import (
	"errors"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/project-iris/iris/proto/link"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/session"
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
	drop chan struct{}   // Channel sync for remote drop on graceful tear-down
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
		drop: make(chan struct{}, 2),
	}
}

// Starts the inbound message processor and router.
func (p *peer) Start() {
	go p.processor(p.conn.CtrlLink)
	go p.processor(p.conn.DataLink)
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

	// Sync the processor terminations and return
	errc := make(chan error)
	for i := 0; i < 2; i++ {
		p.quit <- errc
		if err := <-errc; res != nil {
			res = err
		}
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
	case <-time.After(config.PastrySendTimeout):
		return errors.New("timeout")
	}
}

// Accepts inbound messages and routes them into the overlay.
func (p *peer) processor(link *link.Link) {
	var errc chan error
	var errv error

	// Retrieve messages until connection is torn down or termination is requested
	for closed := false; !closed && errc == nil; {
		select {
		case errc = <-p.quit:
			// This should not happen often (graceful close on the recv channel instead)
			continue
		case msg, ok := <-link.Recv:
			if !ok {
				// Connection went down (gracefully or not, dunno)
				closed = true
				continue
			}
			// Route the control message
			p.owner.route(p, msg)
		}
	}
	// Signal the overlay of the connection drop
	p.drop <- struct{}{}
	if errc == nil {
		p.owner.drop(p)
	}
	// Report termination result
	if errc == nil {
		errc = <-p.quit
	}
	errc <- errv
}
