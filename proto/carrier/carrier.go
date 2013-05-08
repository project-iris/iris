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

// Package carrier contains the messaging middleware implementation. It is
// currently based on a simplified version of Scribe, where no topic based acls
// are defined.
package carrier

import (
	"crypto/rsa"
	"proto/overlay"
)

// The carrier interface to route all messages to desired topics.
type Carrier interface {
	Boot() error
	Shutdown()

	Subscribe(origin string, topic string) chan *Message
	Unsubscribe(origin string, topic string, ch chan *Message)
	Publish(origin string, topic string, msg *Message)
}

type Message struct {
	Key  []byte
	Iv   []byte
	Data []byte
}

// The real carrier implementation, receiving the overlay events and processing
// them according to the protocol.
type carrier struct {
	transport *overlay.Overlay
}

// Creates a new messaging middleware carrier.
func New(overId string, key *rsa.PrivateKey) Carrier {
	// Create and initialize the overlay
	c := new(carrier)
	c.transport = overlay.New(overId, key, c)

	return Carrier(c)
}

// Boots the message carrier.
func (c *carrier) Boot() error {
	return c.transport.Boot()
}

// Terminates the carrier and all lower layer network primitives.
func (c *carrier) Shutdown() {
	c.transport.Shutdown()
}

// Subscribes to the specified topic and returns a new channel on which incoming
// messages will arrive.
func (c *carrier) Subscribe(origin string, topic string) chan *Message {
	ch := make(chan *Message)

	c.sendSub(origin, overlay.Resolve(topic))

	return ch
}

// Removes the subscription of ch from topic.
func (c *carrier) Unsubscribe(origin string, topic string, ch chan *Message) {
	c.sendUnsub(origin, overlay.Resolve(topic))
}

// Publishes a message into topic.
func (c *carrier) Publish(origin string, topic string, msg *Message) {
	c.sendPub(origin, overlay.Resolve(topic), msg)
}
