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
	"log"
	"math/big"
	"proto/session"
)

// The routing state of a pastry node.
type state struct {
	Leaves []*big.Int
	Routes [][]*big.Int
	Nears  []*big.Int

	Addrs map[string][]string

	Updated uint64
	Merged  uint64
}

// The extra headers the pastry requires to be functional.
type header struct {
	Dest  *big.Int
	State *state
	Meta  []byte
}

// Pastry message container for channel passing.
type message struct {
	head *header
	data *session.Message
}

// Listens on one particular session, extracts the pastry headers out of each
// inbound message and invokes the router to finish the job.
func (o *overlay) receiver(p *peer) {
	defer close(p.out)

	// Read messages until a quit is requested
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
			// Extract the pastry headers
			p.inBuf.Write(pkt.Head.Meta)
			msg := &message{new(header), pkt}
			if err := p.dec.Decode(msg.head); err != nil {
				log.Printf("failed to decode pastry headers: %v.", err)
				return
			}
			log.Printf("routing to: %v.", msg.head.Dest)
			o.route(p, msg)
		}
	}
}

// Sends a pastry join request to the remote peer.
func (o *overlay) sendJoin(p *peer) {
	head := &header{o.nodeId, new(state), nil}
	msg := new(session.Message)
	p.out <- &message{head, msg}
}

// Sends a pastry state message to the remote peer.
func (o *overlay) sendState(p *peer) {
	s := new(state)
	s.Updated = o.time
	s.Merged = p.time

	// Serialize the routing table
	s.Leaves = make([]*big.Int, len(o.leaves))
	for i := 0; i < len(s.Leaves); i++ {
		s.Leaves[i] = new(big.Int).Set(o.leaves[i])
	}
	s.Routes = make([][]*big.Int, len(o.routes))
	for i := 0; i < len(s.Routes); i++ {
		s.Routes[i] = make([]*big.Int, len(o.routes[i]))
		for j := 0; j < len(s.Routes[i]); j++ {
			if id := o.routes[i][j]; id != nil {
				s.Routes[i][j] = new(big.Int).Set(id)
			} else {
				// TODO: Hack until bug gets fixed https://code.google.com/p/go/issues/detail?id=5305
				s.Routes[i][j] = new(big.Int)
			}
		}
	}
	s.Nears = make([]*big.Int, len(o.nears))
	for i := 0; i < len(s.Nears); i++ {
		s.Nears[i] = new(big.Int).Set(o.nears[i])
	}
	// Serialize the address list
	s.Addrs = make(map[string][]string)
	for id, p := range o.pool {
		addrs := make([]string, len(p.addrs))
		copy(addrs, p.addrs)
		s.Addrs[id] = addrs
	}
	// Send everythin over the wire
	head := &header{o.nodeId, s, nil}
	msg := new(session.Message)
	p.out <- &message{head, msg}
}

// Waits for outbound pastry messages, encodes them into the lower level session
// format and send them on their way.
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
			// Check whether header recode is needed and do if so
			if msg.head != nil {
				if err := p.enc.Encode(msg.head); err != nil {
					log.Printf("failed to encode pastry headers: %v.", err)
					return
				}
				msg.data.Head.Meta = append(msg.data.Head.Meta[:0], p.outBuf.Bytes()...)
				p.outBuf.Reset()
			}
			p.netOut <- msg.data
		}
	}
}
