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
	opBal
	opRep
	opDir
)

// Extra headers for the carrier.
type header struct {
	// Always present fields
	Meta    interface{} // Additional upper layer headers
	Op      opcode      // The operation to execute
	SrcNode *big.Int    // Origin overlay node
	SrcApp  *big.Int    // Origin application within the origin node

	// Operation dependent fields
	DestApp *big.Int // Destination app used during direct messaging
	Topic   *big.Int // Topic id used during unsubscribing, broadcasting and balancing
	Prev    *big.Int // Previous hop inside topic to prevent optimize routes
	Report  *report  // CPU load/capacity report
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

// Assembles a subscription message and sends it towards the topic root.
func (c *carrier) sendSubscribe(topicId *big.Int) {
	msg := &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:      opSub,
				SrcNode: c.transport.Self(),
			},
		},
	}
	c.transport.Send(topicId, msg)
}

// Assembles an unsubscription message and sends it to dest.
func (c *carrier) sendUnsubscribe(dest, topic *big.Int) {
	msg := &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:      opUnsub,
				SrcNode: c.transport.Self(),
				Topic:   topic,
			},
		},
	}
	c.transport.Send(dest, msg)
}

// Publishes a message into a topic.
func (c *carrier) sendPublish(app, topic *big.Int, msg *session.Message) {
	msg.Head.Meta = &header{
		Meta:    msg.Head.Meta,
		Op:      opPub,
		SrcNode: c.transport.Self(),
		SrcApp:  app,
		Topic:   topic,
	}
	c.transport.Send(topic, msg)
}

// Forwards a published message inside the topic to dest.
func (c *carrier) fwdPublish(dest *big.Int, msg *session.Message) {
	msg.Head.Meta.(*header).Prev = c.transport.Self()
	c.transport.Send(dest, msg)
}

// Requests a message balancing in the topic identified by dest.
func (c *carrier) sendBalance(src *big.Int, dest *big.Int, msg *session.Message) {
	msg.Head.Meta = &header{
		Meta:    msg.Head.Meta,
		Op:      opBal,
		SrcNode: c.transport.Self(),
		SrcApp:  src,
		Topic:   dest,
	}
	c.transport.Send(dest, msg)
}

// Forwards a balanced message to dest for remote balancing.
func (c *carrier) fwdBalance(dest *big.Int, msg *session.Message) {
	msg.Head.Meta.(*header).Prev = c.transport.Self()
	c.transport.Send(dest, msg)
}

// Sends out a load report to a remote carrier node.
func (c *carrier) sendReport(dest *big.Int, rep *report) {
	msg := &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:      opRep,
				SrcNode: c.transport.Self(),
				Report:  rep,
			},
		},
	}
	c.transport.Send(dest, msg)
}

// Sends out a message directed to a specific node and app.
func (c *carrier) sendDirect(src *big.Int, dest *Address, msg *session.Message) {
	msg.Head.Meta = &header{
		Meta:    msg.Head.Meta,
		Op:      opDir,
		SrcNode: c.transport.Self(),
		SrcApp:  src,
		DestApp: dest.appId,
	}
	c.transport.Send(dest.nodeId, msg)
}
