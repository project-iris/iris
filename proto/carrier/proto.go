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

// Contains the wire protocol for the carrier layer communication.

package carrier

import (
	"encoding/gob"
	"github.com/karalabe/iris/proto"
	"math/big"
)

// Carrier operation code type.
type opcode uint8

// Carrier operation types.
const (
	opSub   opcode = iota // Carrier subscription
	opUnsub               // Carrier subscription removal
	opPub                 // Topic publish
	opBal                 // Topic balance
	opRep                 // Load report
	opDir                 // Direct send
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

// Envelopes a carrier header into the generic packet container and sends it to
// its destination via the overlay transport.
func (c *carrier) sendPacket(dest *big.Int, head *header) {
	// Add the origin node
	head.SrcNode = c.transport.Self()

	// Assemble and send the final message
	msg := &proto.Message{
		Head: proto.Header{
			Meta: head,
		},
	}
	c.transport.Send(dest, msg)
}

// Envelopes a carrier header into an existing packet container and sends it to
// its destination via the overlay transport.
func (c *carrier) sendDataPacket(dest *big.Int, head *header, msg *proto.Message) {
	// Add the origin node and envelope the original meta
	head.SrcNode = c.transport.Self()
	head.Meta = msg.Head.Meta

	// Insert the new header and fire away
	msg.Head.Meta = head
	c.transport.Send(dest, msg)
}

// Forwards a carrier message to a new destination, leaving the original message
// intact, except inserting the local node as the previous hop.
func (c *carrier) fwdDataPacket(dest *big.Int, msg *proto.Message) {
	msg.Head.Meta.(*header).Prev = c.transport.Self()
	c.transport.Send(dest, msg)
}

// Assembles a subscription message, consisting of the subscribe opcode and send
// it towards the destination topic.
func (c *carrier) sendSubscribe(topicId *big.Int) {
	c.sendPacket(topicId, &header{Op: opSub})
}

// Assembles an unsubscription message, consisting of the unsubscribe opcode
// and desired topic to drop. The message is sent to the parent node in the
// topic subtree.
func (c *carrier) sendUnsubscribe(parentId *big.Int, topicId *big.Int) {
	c.sendPacket(parentId, &header{Op: opUnsub, Topic: topicId})
}

// Assembles a topic publish message, consisting of the publish opcode, the
// originating application (to allow replies) and the destination topic (to
// allow catching publishes midway).
func (c *carrier) sendPublish(appId *big.Int, topicId *big.Int, msg *proto.Message) {
	c.sendDataPacket(topicId, &header{Op: opPub, SrcApp: appId, Topic: topicId}, msg)
}

// Reroutes a publish message to a new destination to traverse the topic tree
// directly instead of going up till he root and back down.
func (c *carrier) fwdPublish(dest *big.Int, msg *proto.Message) {
	c.fwdDataPacket(dest, msg)
}

// Assembles a topic balance message, consisting of the balance opcode, the
// originating application (to allow replies) and the destination topic (to
// allow catching balances midway).
func (c *carrier) sendBalance(appId *big.Int, topicId *big.Int, msg *proto.Message) {
	c.sendDataPacket(topicId, &header{Op: opBal, SrcApp: appId, Topic: topicId}, msg)
}

// Reroutes a balanced message to a new destination to traverse the topic tree
// directly instead of going up till he root and back down.
func (c *carrier) fwdBalance(dest *big.Int, msg *proto.Message) {
	c.fwdDataPacket(dest, msg)
}

// Assembles a carrier load report message and sends it to a peer.
func (c *carrier) sendReport(nodeId *big.Int, rep *report) {
	c.sendPacket(nodeId, &header{Op: opRep, Report: rep})
}

// Sends out a message directed to a specific node and app.
func (c *carrier) sendDirect(srcAppId *big.Int, destAddr *Address, msg *proto.Message) {
	c.sendDataPacket(destAddr.nodeId, &header{Op: opDir, SrcApp: srcAppId, DestApp: destAddr.appId}, msg)
}
