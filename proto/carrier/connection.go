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

// This file contains the connection through which to access the carrier.

package carrier

import (
	"fmt"
	"github.com/karalabe/iris/proto/overlay"
	"github.com/karalabe/iris/proto/session"
	"log"
	"math/big"
	"math/rand"
	"sync"
)

// A remote application address.
type Address struct {
	nodeId *big.Int
	appId  *big.Int
}

// A carrier adderss-tagged message.
type Message struct {
	Src *Address
	Msg *session.Message
}

// A carrier connection through which to communicate.
type Connection struct {
	id    *big.Int           // The id of the application
	relay *carrier           // The carrier through which to communicate
	call  ConnectionCallback // The callback to notify of incoming messages

	subs map[string]string // Mapping from ids to original names

	lock sync.RWMutex
}

// Callback methods for the carrier connection events.
type ConnectionCallback interface {
	HandleDirect(src *Address, msg *session.Message)
	HandlePublish(src *Address, topic string, msg *session.Message)
}

// Creates a new application through which to communicate with others.
func (c *carrier) Connect(cb ConnectionCallback) *Connection {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Create the new application interface and store
	conn := &Connection{
		id:    big.NewInt(rand.Int63()),
		relay: c,
		call:  cb,
		subs:  make(map[string]string),
	}
	c.conns[conn.id.String()] = conn

	return conn
}

// Closes a carrier connection, removing it from the active list.
func (c *Connection) Close() {
	c.relay.lock.Lock()
	defer c.relay.lock.Unlock()

	delete(c.relay.conns, c.id.String())
}

// Subscribes to the specified topic and returns a new channel on which incoming
// messages will arrive.
func (c *Connection) Subscribe(topic string) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Resolve the topic id and its string form
	key := overlay.Resolve(topic)
	skey := key.String()

	// Create c subscription mapping if new
	if _, ok := c.subs[skey]; ok {
		return fmt.Errorf("already subscribed")
	}
	c.subs[skey] = topic

	// Subscribe the app to the topic and initiate a subscription message
	c.relay.handleSubscribe(c.id, key, true)
	c.relay.sendSubscribe(key)

	return nil
}

// Removes the subscription from topic.
func (c *Connection) Unsubscribe(topic string) {
	// Remove the carrier subscription
	key := overlay.Resolve(topic)
	c.relay.handleUnsubscribe(c.id, key, true)

	// Clean up the application
	c.lock.Lock()
	defer c.lock.Unlock()

	skey := key.String()
	if _, ok := c.subs[skey]; ok {
		delete(c.subs, key.String())
	}
}

// Publishes a message into topic to be broadcast to everyone.
func (c *Connection) Publish(topic string, msg *session.Message) {
	c.relay.sendPublish(c.id, overlay.Resolve(topic), msg)
}

// Delivers a message to a subscribed node, balancing amongst all subscriptions.
func (c *Connection) Balance(topic string, msg *session.Message) {
	c.relay.sendBalance(c.id, overlay.Resolve(topic), msg)
}

// Sends a direct message to a known app on a known node.
func (c *Connection) Direct(dest *Address, msg *session.Message) {
	c.relay.sendDirect(c.id, dest, msg)
}

// Delivers a topic subscription to the application.
func (c *Connection) deliverPublish(src *Address, topic *big.Int, msg *session.Message) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if top, ok := c.subs[topic.String()]; ok {
		c.call.HandlePublish(src, top, msg)
	} else {
		log.Printf("publish on a non-subscribed topic: %v.", topic)
	}
}

// Delivers a direct message to the application.
func (c *Connection) deliverDirect(src *Address, msg *session.Message) {
	c.call.HandleDirect(src, msg)
}
