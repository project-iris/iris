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

// File containing the inter-node communication methods. For every connection
// two separate go routines are started: a receiver that accepts inbound packets
// executing the routing on the same thread and a sender which moves messages
// from the application channel to the network socket. Network errors are
// detected by the receiver, which notifies the overlay.

package overlay

import (
	"config"
	"fmt"
	"log"
	"math/big"
	"proto/session"
	"time"
)

// Routing state exchange message (leaves, neighbors and common row).
type state struct {
	Addrs   map[string][]string
	Updated uint64
	Repair  bool
	Passive bool
}

// Extra headers for the overlay: destination id for routing, state for routing
// table exchanges and meta for upper layer application use.
type header struct {
	Dest  *big.Int
	State *state
	Meta  []byte
}

// Overlay message container for channel passing.
type message struct {
	head *header
	data *session.Message
}

// Listens on one particular session, extracts the overlay headers out of each
// inbound message and invokes the router to finish the job. The thread stops at
// either overlay termination, connection termination, network error or packet
// format error.
func (o *overlay) receiver(p *peer) {
	defer func() { o.dropSink <- p }()
	for {
		select {
		case <-o.quit:
			return
		case <-p.quit:
			return
		case pkt, ok := <-p.netIn:
			if !ok {
				return
			}
			// Extract the overlay headers
			p.inBuf.Write(pkt.Head.Meta)
			msg := &message{new(header), pkt}
			if err := p.dec.Decode(msg.head); err != nil {
				log.Printf("failed to decode headers: %v.", err)
				return
			}
			o.route(p, msg)
		}
	}
}

// Waits for outbound overlay messages, encodes them into the lower level
// session format and sends them on their way. The thread stops at either
// overlay termination, connection termination, application outboung channel
// close or network timeout.
func (o *overlay) sender(p *peer) {
	defer close(p.netOut)
	for {
		select {
		case <-o.quit:
			return
		case <-p.quit:
			return
		case msg, ok := <-p.out:
			if !ok {
				return
			}
			// Check whether header recode is needed (i.e. optimized forwarding)
			if msg.head != nil {
				if err := p.enc.Encode(msg.head); err != nil {
					panic(fmt.Sprintf("failed to encode headers: %v.", err))
				}
				msg.data.Head.Meta = append(msg.data.Head.Meta[:0], p.outBuf.Bytes()...)
				p.outBuf.Reset()
			}
			// Send the packet but prevent infinite blocking
			select {
			case <-o.quit:
				return
			case <-p.quit:
				return
			case p.netOut <- msg.data:
				// All's fine and boring
			}
		}
	}
}

// Sends an already assembled message m to peer p. To prevent the system from
// locking up due to a slow peer, p is dropped if a timeout is reached. Quit
// events are also checked to ensure a close immediately notifies all senders.
func (o *overlay) send(m *message, p *peer) {
	timeout := time.Tick(time.Duration(config.OverlaySendTimeout) * time.Millisecond)
	select {
	case <-o.quit:
		return
	case <-p.quit:
		return
	case <-timeout:
		o.dropSink <- p
		return
	case p.out <- m:
		// Ok, we're happy
	}
}

// Sends an overlay join message to the remote peer, which is a simple state
// package having 0 as the update time and containing only the local addresses.
func (o *overlay) sendJoin(p *peer) {
	s := new(state)
	s.Addrs = make(map[string][]string)

	// Ensure nodes can contact joining peer
	o.lock.RLock()
	s.Addrs[o.nodeId.String()] = o.addrs
	o.lock.RUnlock()

	o.send(&message{&header{o.nodeId, s, nil}, new(session.Message)}, p)
}

// Sends an overlay state message to the remote peer and optionally may request a
// state update in response (route repair).
func (o *overlay) sendState(p *peer, repair bool) {
	s := new(state)
	s.Addrs = make(map[string][]string)
	s.Repair = repair

	o.lock.RLock()
	s.Updated = o.time

	// Serialize the leaf set, common row and neighbor list into the address map.
	// Make sure all entries are checked for existence to avoid a race condition
	// with node dropping vs. table updates.
	s.Addrs[o.nodeId.String()] = o.addrs
	for _, id := range o.routes.leaves {
		if id.Cmp(o.nodeId) != 0 {
			sid := id.String()
			if node, ok := o.pool[sid]; ok {
				s.Addrs[sid] = node.addrs
			}
		}
	}
	idx, _ := prefix(o.nodeId, p.self)
	for _, id := range o.routes.routes[idx] {
		if id != nil {
			sid := id.String()
			if node, ok := o.pool[sid]; ok {
				s.Addrs[sid] = node.addrs
			}
		}
	}
	for _, id := range o.routes.nears {
		sid := id.String()
		if node, ok := o.pool[sid]; ok {
			s.Addrs[sid] = node.addrs
		}
	}
	o.lock.RUnlock()
	o.send(&message{&header{o.nodeId, s, nil}, new(session.Message)}, p)
}

// Sends a heartbeat message, tagging whether the connection is an active route
// entry or not.
func (o *overlay) sendBeat(p *peer, passive bool) {
	s := new(state)
	s.Passive = passive

	o.lock.RLock()
	s.Updated = o.time
	o.lock.RUnlock()

	o.send(&message{&header{o.nodeId, s, nil}, new(session.Message)}, p)
}
