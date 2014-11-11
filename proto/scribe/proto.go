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

// Contains the wire protocol for the scribe layer communication.

package scribe

import (
	"encoding/gob"
	"math/big"

	"github.com/project-iris/iris/proto"
)

// Scribe operation code type.
type opcode uint8

// Scribe operation types.
const (
	opSubscribe   opcode = iota // Group subscription
	opUnsubscribe               // Group subscription removal
	opPublish                   // Event publish
	opBalance                   // Topic balance
	opReport                    // Load report
	opDirect                    // Direct send
)

// Extra headers for the scribe.
type header struct {
	// Always present fields
	Meta   interface{} // Additional upper layer headers
	Op     opcode      // The operation to execute
	Sender *big.Int    // Origin overlay node

	// Operation dependent fields
	Topic  *big.Int // Topic id used during unsubscribing, broadcasting and balancing
	Prev   *big.Int // Previous hop inside topic to prevent optimize routes
	Report *report  // CPU load/capacity report
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

// Envelopes a scribe header into the generic packet container and sends it to
// its destination via the overlay transport.
func (o *Overlay) sendPacket(dest *big.Int, head *header) {
	// Add the origin node
	head.Sender = o.pastry.Self()

	// Assemble and send the final message
	msg := &proto.Message{
		Head: proto.Header{
			Meta: head,
		},
	}
	o.pastry.Send(dest, msg)
}

// Envelopes a scribe header into an existing packet container and sends it to
// its destination via the overlay transport.
func (o *Overlay) sendDataPacket(dest *big.Int, head *header, msg *proto.Message) {
	// Add the origin node and envelope the original meta
	head.Sender = o.pastry.Self()
	head.Meta = msg.Head.Meta

	// Insert the new header and fire away
	msg.Head.Meta = head
	o.pastry.Send(dest, msg)
}

// Forwards a scribe message to a new destination, leaving the original message
// intact, except inserting the local node as the previous hop.
func (o *Overlay) fwdDataPacket(dest *big.Int, msg *proto.Message) {
	msg.Head.Meta.(*header).Prev = o.pastry.Self()
	o.pastry.Send(dest, msg)
}

// Assembles a subscription message, consisting of the subscribe opcode and send
// it towards the destination topic.
func (o *Overlay) sendSubscribe(topicId *big.Int) {
	o.sendPacket(topicId, &header{Op: opSubscribe})
}

// Assembles an unsubscription message, consisting of the unsubscribe opcode
// and desired topic to drop. The message is sent to the parent node in the
// topic subtree.
func (o *Overlay) sendUnsubscribe(parentId *big.Int, topicId *big.Int) {
	o.sendPacket(parentId, &header{Op: opUnsubscribe, Topic: topicId})
}

// Assembles a topic publish message, consisting of the publish opcode, and the
// destination topic (to allow catching publishes in flight).
func (o *Overlay) sendPublish(topicId *big.Int, msg *proto.Message) {
	o.sendDataPacket(topicId, &header{Op: opPublish, Topic: topicId}, msg)
}

// Reroutes a publish message to a new destination to traverse the topic tree
// directly instead of going up till he root and back down.
func (o *Overlay) fwdPublish(dest *big.Int, msg *proto.Message) {
	o.fwdDataPacket(dest, msg)
}

// Assembles a topic balance message, consisting of the balance opcode, the
// originating application (to allow replies) and the destination topic (to
// allow catching balances midway).
func (o *Overlay) sendBalance(topicId *big.Int, msg *proto.Message) {
	o.sendDataPacket(topicId, &header{Op: opBalance, Topic: topicId}, msg)
}

// Reroutes a balanced message to a new destination to traverse the topic tree
// directly instead of going up till he root and back down.
func (o *Overlay) fwdBalance(dest *big.Int, msg *proto.Message) {
	o.fwdDataPacket(dest, msg)
}

// Assembles a scribe load report message and sends it to a peer.
func (o *Overlay) sendReport(nodeId *big.Int, rep *report) {
	o.sendPacket(nodeId, &header{Op: opReport, Report: rep})
}

// Sends out a message directed to a specific node.
func (o *Overlay) sendDirect(dest *big.Int, msg *proto.Message) {
	o.sendDataPacket(dest, &header{Op: opDirect}, msg)
}
