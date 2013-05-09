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

package carrier

import (
	"encoding/gob"
	"math/big"
	"proto/session"
)

// Carrier operation code type.
type opcode uint8

// Carrier operation types.
const (
	opSub opcode = iota
	opUnsub
	opPub
	opBcast
)

// Extra headers for the carrier.
type header struct {
	Meta interface{}

	// The operation to execute
	Op opcode

	// Sender ids to allow direct reply
	SrcNode *big.Int
	SrcApp  *big.Int

	// Destination topic OR app
	Dest *big.Int
}

// Creates a copy of the header needed by the broadcast.
func (h *header) copy() *header {
	cpy := new(header)
	*cpy = *h
	return cpy
}

// Make sure the header struct is registered with gob.
func init() {
	gob.Register(&header{})
}

// Broadcasts and already assembled message.
func (c *carrier) sendBcast(dest, topic *big.Int, msg *session.Message) {
	msg.Head.Meta.(*header).Op = opBcast
	msg.Head.Meta.(*header).Dest = topic
	c.transport.Send(dest, msg)
}

// Assembles a subscription message and sends it towards its destination.
func (c *carrier) sendSub(dest *big.Int) {
	msg := &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:      opSub,
				SrcNode: c.transport.Self(),
			},
		},
	}
	c.transport.Send(dest, msg)
}

// Assembles an unsubscription message and sends it towards its destination.
func (c *carrier) sendUnsub(dest *big.Int) {
	msg := &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:      opUnsub,
				SrcNode: c.transport.Self(),
			},
		},
	}
	c.transport.Send(dest, msg)
}

// Publishes a message towards the topic identified by dest.
func (c *carrier) sendPub(src *big.Int, dest *big.Int, msg *session.Message) {
	// Envelope the upper meta into the carrier headers
	head := &header{
		Meta:    msg.Head.Meta,
		Op:      opPub,
		SrcNode: c.transport.Self(),
		SrcApp:  src,
	}
	msg.Head.Meta = head

	c.transport.Send(dest, msg)
}

// Implements the overlay.Callback.Deliver method.
func (c *carrier) Deliver(msg *session.Message, key *big.Int) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opSub:
		// Accept remote subscription if not self-passthrough
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.subscribe(head.SrcNode, key, false)
		}
	case opUnsub:
		// Accept remote unsubscription if not self-passthrough
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.unsubscribe(head.SrcNode, key, false)
		}
	case opPub:
		// Topic root, start broadcast
		c.broadcast(msg, key)
	case opBcast:
		// Topic forwarder, also broadcast
		c.broadcast(msg, head.Dest)
	}
}

// Implements the overlay.Callback.Forward method.
func (c *carrier) Forward(msg *session.Message, key *big.Int) bool {
	// Pick out subscriptions, forward anything else
	head := msg.Head.Meta.(*header)
	if head.Op == opSub {
		// Accept remote subscription if not self-passthrough
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.subscribe(head.SrcNode, key, false)
		}
		// Swap out the originating node to the current and forward
		head.SrcNode = c.transport.Self()
		return true
	}
	return true
}
