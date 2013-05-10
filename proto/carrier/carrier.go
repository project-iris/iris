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
	"fmt"
	"log"
	"math/big"
	"proto/overlay"
	"proto/session"
	"sync"
)

// The carrier interface to route all messages to desired topics.
type Carrier interface {
	Boot() error
	Shutdown()

	Subscribe(id *big.Int, topic string) chan *session.Message
	Unsubscribe(id *big.Int, topic string)
	Publish(id *big.Int, topic string, msg *session.Message)
	Balance(id *big.Int, topic string, msg *session.Message)
}

// The real carrier implementation, receiving the overlay events and processing
// them according to the protocol.
type carrier struct {
	transport *overlay.Overlay
	topics    map[string]*topic

	lock sync.RWMutex
}

// Creates a new messaging middleware carrier.
func New(overId string, key *rsa.PrivateKey) Carrier {
	// Create and initialize the overlay
	c := &carrier{
		topics: make(map[string]*topic),
	}
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
func (c *carrier) Subscribe(id *big.Int, topic string) chan *session.Message {
	key := overlay.Resolve(topic)

	c.subscribe(id, key, true)
	c.sendSub(key)

	return nil
}

// Removes the subscription of ch from topic.
func (c *carrier) Unsubscribe(id *big.Int, topic string) {
	c.sendUnsub(overlay.Resolve(topic))
}

// Publishes a message into topic.
func (c *carrier) Publish(id *big.Int, topic string, msg *session.Message) {
	c.sendPub(id, overlay.Resolve(topic), msg)
}

// Delivers a message to a subscribed node, balancing amongst all subscriptions.
func (c *carrier) Balance(id *big.Int, topic string, msg *session.Message) {
	c.sendBal(id, overlay.Resolve(topic), msg)
}

// Handles the subscription event to a topic.
func (c *carrier) subscribe(id, topic *big.Int, app bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the requested topic exists, then subscribe
	sid := topic.String()
	top, ok := c.topics[sid]
	if !ok {
		top = newTopic()
		c.topics[sid] = top
	}
	// Depending whether local or remote subscription, execute accordingly
	top.subscribe(id, app)
}

// Handles the unsubscription event from a topic.
func (c *carrier) unsubscribe(id, topic *big.Int, app bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Unsubscribe from the topic and remove if empty
	sid := topic.String()
	if top, ok := c.topics[sid]; ok {
		if top.unsubscribe(id, app) {
			delete(c.topics, sid)
		}
	}
}

// Handles the broadcast event of a topic.
func (c *carrier) broadcast(msg *session.Message, topic *big.Int) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Fetch the topic to operate on
	sid := topic.String()
	top, ok := c.topics[sid]
	if !ok {
		log.Printf("unknown topic %v, discarding broadcast %v.", topic, msg)
		return
	}
	// Send to all in the topic
	top.lock.RLock()
	defer top.lock.RUnlock()

	// Send to all remote nodes (copy for lower layer safety)
	for _, node := range top.nodes {
		cpy := new(session.Message)
		*cpy = *msg
		cpy.Head.Meta = msg.Head.Meta.(*header).copy()

		c.sendBcast(node, topic, cpy)
	}
	// Send to all local apps (copy for lower layer safety)
	for _, app := range top.apps {
		cpy := new(session.Message)
		*cpy = *msg
		cpy.Head.Meta = msg.Head.Meta.(*header).Meta

		fmt.Printf("Delivering broadcast message to local %v: %v\n", app, cpy)
	}
}

// Handles the load balancing event of a topic.
func (c *carrier) balance(msg *session.Message, topic *big.Int, prev *big.Int) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if top, ok := c.topics[topic.String()]; ok {
		// Fetch the recipient and either forward or deliver
		node, app := top.balance(prev)
		if node != nil {
			c.fwdBal(node, msg)
		} else {
			fmt.Printf("Delivering balanced message to local %v: %v\n", app, msg)
		}
		return true
	}
	return false
}
