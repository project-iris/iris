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
	"config"
	"fmt"
	"math/big"
	"proto/session"
)

// Carrier operation types.
const (
	opSub byte = iota
	opUnsub
	opPub
)

var idSize = config.OverlaySpace / 8

// Encodes the op, node id and app id into a single byte buffer:
//   - First byte: operation
//   - Next fixed bytes: node overlay id
//   - Final variable bytes: application id
func (c *carrier) encode(op byte, app string) []byte {
	appId := []byte(app)
	nodeId := c.transport.Self().Bytes()

	data := make([]byte, 1+idSize+len(appId))

	data[0] = op
	copy(data[1+idSize-len(nodeId):], nodeId)
	copy(data[len(data)-len(app):], app)

	return data
}

// Decodes the op, node id and app id from a byte buffer
func (c *carrier) decode(data []byte) (byte, *big.Int, string) {
	op := data[0]
	node := new(big.Int).SetBytes(data[1 : 1+idSize])
	app := string(data[1+idSize:])
	return op, node, app
}

// Assembles a subscription message and sends it towards its destination.
func (c *carrier) sendSub(src string, dest *big.Int) {
	head := session.Header{Meta: c.encode(opSub, src)}
	msg := &session.Message{Head: head}
	c.transport.Send(dest, msg)
}

// Assembles an unsubscription message and sends it towards its destination.
func (c *carrier) sendUnsub(src string, dest *big.Int) {
	head := session.Header{Meta: c.encode(opUnsub, src)}
	msg := &session.Message{Head: head}
	c.transport.Send(dest, msg)
}

// Publishes msg towards the topic identified by dest.
func (c *carrier) sendPub(src string, dest *big.Int, msg *Message) {
	packet := &session.Message{
		Head: session.Header{
			Meta: c.encode(opPub, src),
			Key:  msg.Key,
			Iv:   msg.Iv,
		},
		Data: msg.Data,
	}
	c.transport.Send(dest, packet)
}

// Implements the overlay.Callback.Deliver method.
func (c *carrier) Deliver(msg *session.Message, key *big.Int) {
	op, node, app := c.decode(msg.Head.Meta)
	switch op {
	case opSub:
		fmt.Printf("%v:%v sub to %v\n", node, app, key)
	case opUnsub:
		fmt.Printf("%v:%v unsub to %v\n", node, app, key)
	case opPub:
		fmt.Printf("%v:%v pub %v from %v\n", node, app, msg.Data, key)
	}
}

// Implements the overlay.Callback.Forward method.
func (c *carrier) Forward(msg *session.Message, key *big.Int, src *big.Int) bool {

	// REWRITE RWERITE BLA BLA BE

	// If the message is a data container, let itt pass
	if len(msg.Data) != 0 {
		return true
	}
	// If it's a subscribe, process and let it cascade
	if msg.Head.Meta[0] == opSub {
		fmt.Printf("Cascade subscribe\n")
		return false
	}
	// If it's an unsubscribe,
	// Process other events and discard them
	if msg.Head.Meta[0] == opSub {
		fmt.Printf("Stopping subscribe\n")
	} else {
		fmt.Printf("Stopping unsubscribe\n")
	}
	return false
}
