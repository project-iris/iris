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

// This file contains the connection through which to access the carrier.

package carrier

import (
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"sync"

	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/pastry"
)

// A remote application address.
type Address struct {
	nodeId *big.Int // Overlay node id
	appId  *big.Int // Carrier app id
}

// A carrier connection through which to communicate.
type Connection struct {
	id      *big.Int           // The id of the application
	carrier *carrier           // The carrier through which to communicate
	call    ConnectionCallback // The callback to notify of incoming messages

	subLive map[string]string // Active subscriptions, mapped from id to original name
	subLock sync.RWMutex      // Mutex to protect the subscription map
}

// Callback methods for the carrier connection events.
type ConnectionCallback interface {
	HandleDirect(src *Address, msg *proto.Message)
	HandlePublish(src *Address, topic string, msg *proto.Message)
}

// Creates a new application through which to communicate with others.
func (c *carrier) Connect(callback ConnectionCallback) *Connection {
	// Create the new application interface
	conn := &Connection{
		id:      big.NewInt(rand.Int63()),
		carrier: c,
		call:    callback,
		subLive: make(map[string]string),
	}

	// Store the application into the carrier pool
	c.lock.Lock()
	c.conns[conn.id.String()] = conn
	c.lock.Unlock()

	return conn
}

// Disconnects an application with a given id by simply dropping it from the
// pool of active connections.
func (c *carrier) disconnect(id *big.Int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.conns, id.String())
}

// Closes a carrier connection, removing it from the active list.
func (c *Connection) Close() {
	// Unsubscribe all active subscriptions
	c.subLock.RLock()
	for _, topic := range c.subLive {
		c.carrier.handleUnsubscribe(c.id, pastry.Resolve(topic), true)
	}
	c.subLive = nil
	c.subLock.RUnlock()

	// Drop the connection
	c.carrier.disconnect(c.id)
}

// Subscribes to the specified topic and returns a new channel on which incoming
// messages will arrive.
func (c *Connection) Subscribe(topic string) error {
	// Resolve the topic id and its string form
	key := pastry.Resolve(topic)
	skey := key.String()

	// Create a subscription mapping if new
	c.subLock.Lock()
	if _, ok := c.subLive[skey]; ok {
		c.subLock.Unlock()
		return fmt.Errorf("already subscribed")
	}
	c.subLive[skey] = topic
	c.subLock.Unlock()

	// Subscribe the app to the topic and initiate a subscription message
	c.carrier.handleSubscribe(c.id, key, true)
	c.carrier.sendSubscribe(key)

	return nil
}

// Removes the subscription from topic.
func (c *Connection) Unsubscribe(topic string) {
	// Remove the carrier subscription
	key := pastry.Resolve(topic)
	c.carrier.handleUnsubscribe(c.id, key, true)

	// Clean up the application
	c.subLock.Lock()
	skey := key.String()
	if _, ok := c.subLive[skey]; ok {
		delete(c.subLive, skey)
	}
	c.subLock.Unlock()
}

// Publishes a message into topic to be broadcast to everyone.
func (c *Connection) Publish(topic string, msg *proto.Message) {
	if err := msg.Encrypt(); err != nil {
		log.Printf("carrier: failed to encrypt publish message: %v.\n", err)
	}
	c.carrier.sendPublish(c.id, pastry.Resolve(topic), msg)
}

// Delivers a message to a subscribed node, balancing amongst all subscriptions.
func (c *Connection) Balance(topic string, msg *proto.Message) {
	if err := msg.Encrypt(); err != nil {
		log.Printf("carrier: failed to encrypt balance message: %v.\n", err)
	}
	c.carrier.sendBalance(c.id, pastry.Resolve(topic), msg)
}

// Sends a direct message to a known app on a known node.
func (c *Connection) Direct(dest *Address, msg *proto.Message) {
	if err := msg.Encrypt(); err != nil {
		log.Printf("carrier: failed to encrypt direct message: %v.\n", err)
	}
	c.carrier.sendDirect(c.id, dest, msg)
}

// Delivers a topic subscription to the application.
func (c *Connection) deliverPublish(src *Address, topic *big.Int, msg *proto.Message) {
	c.subLock.RLock()
	top, ok := c.subLive[topic.String()]
	c.subLock.RUnlock()

	if ok {
		c.call.HandlePublish(src, top, msg)
	} else {
		// Simple race condition between unsubscribe and publish, left if for debug
		// log.Printf("carrier: publish on a non-subscribed topic %v: %v.", topic, msg)
	}
}

// Delivers a direct message to the application.
func (c *Connection) deliverDirect(src *Address, msg *proto.Message) {
	c.call.HandleDirect(src, msg)
}
