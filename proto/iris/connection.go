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

package iris

import (
	"fmt"
	"github.com/karalabe/iris/proto/carrier"
	"sync"
	"time"
)

var appPrefix = "app:"
var topPrefix = "top:"

type connection struct {
	// Application layer fields
	app     string              // Connection identifier
	handler ConnectionHandler   // Handler for connection events
	conn    *carrier.Connection // Interface into the distributed carrier

	reqIdx  uint64                 // Index to assign the next request
	reqPend map[uint64]chan []byte // Active requests waiting for a reply
	reqLock sync.RWMutex           // Mutex to protect the request map

	subLive map[string]SubscriptionHandler // Active subscriptions
	subLock sync.RWMutex                   // Mutex to protect the subscription map

	tunIdx uint64             // Index to assign the next tunnel
	tuns   map[uint64]*tunnel // Active tunnels

	lock sync.Mutex

	// Bookkeeping fields
	quit chan chan error // Quit channel to synchronize termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

func Connect(carrier carrier.Carrier, app string, handler ConnectionHandler) Connection {
	// Create the connection object
	c := &connection{
		app: app,

		reqPend: make(map[uint64]chan []byte),
		subLive: make(map[string]SubscriptionHandler),
		//tunPend: make(map[uint64]iris.Tunnel),
		//tunInit: make(map[uint64]chan struct{}),
		//tunLive: make(map[uint64]*tunnel),

		tuns:    make(map[uint64]*tunnel),
		handler: handler,

		// Bookkeeping
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	c.conn = carrier.Connect(c)
	c.conn.Subscribe(appPrefix + app)

	return c
}

// Implements iris.Connection.Broadcast. Tags the destination id with the app
// prefix to ensure apps and topics can overlap, bundles the payload into a
// protocol packet and publishes it.
func (c *connection) Broadcast(app string, msg []byte) error {
	c.conn.Publish(appPrefix+app, assembleBroadcast(msg))
	return nil
}

// Implements iris.Connection.Request. Tags the destination with the app prefix,
// bundles the request and asks the carrier to balance it to a suitable place,
// and finally starts the countdown.
func (c *connection) Request(app string, req []byte, timeout time.Duration) ([]byte, error) {
	// Create a reply channel for the results
	c.reqLock.Lock()
	reqCh := make(chan []byte, 1)
	reqId := c.reqIdx
	c.reqIdx++
	c.reqPend[reqId] = reqCh
	c.reqLock.Unlock()

	// Make sure reply channel is cleaned up
	defer func() {
		c.reqLock.Lock()
		defer c.reqLock.Unlock()

		delete(c.reqPend, reqId)
		close(reqCh)
	}()
	// Send the request
	c.conn.Balance(appPrefix+app, assembleRequest(reqId, req, timeout))

	// Retrieve the results, time out or fail if terminating
	select {
	case <-c.term:
		return nil, permError(fmt.Errorf("connection terminating"))
	case <-time.After(timeout):
		return nil, timeError(fmt.Errorf("request timed out"))
	case rep := <-reqCh:
		return rep, nil
	}
}

// Implements iris.Connection.Subscribe. Tags the target with the topic prefix,
// assigns a handler for incoming events and executes a carrier subscription.
func (c *connection) Subscribe(topic string, handler SubscriptionHandler) error {
	// Add the topic prefix to simplify code
	topic = topPrefix + topic

	// Make sure there are no double subscriptions and not closing
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return permError(fmt.Errorf("connection terminating"))
	default:
		if _, ok := c.subLive[topic]; ok {
			c.subLock.Unlock()
			return permError(fmt.Errorf("already subscribed"))
		}
		c.subLive[topic] = handler
	}
	c.subLock.Unlock()

	// Subscribe through the carrier
	return c.conn.Subscribe(topic)
}

// Implements iris.Connection.Publish. Tags the target with the topic prefix,
// bundles the message and requests the carrier to publish it.
func (c *connection) Publish(topic string, msg []byte) error {
	c.conn.Publish(topPrefix+topic, assemblePublish(msg))
	return nil
}

// Implements iris.Connection.Unsubscribe. Removes an active subscription from
// the local list and notifies the carrier of the request.
func (c *connection) Unsubscribe(topic string) error {
	// Add the topic prefix to simplify code
	topic = topPrefix + topic

	// Remove subscription if present
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return permError(fmt.Errorf("connection terminating"))
	default:
		if _, ok := c.subLive[topic]; !ok {
			c.subLock.Unlock()
			return permError(fmt.Errorf("not subscribed"))
		}
	}
	delete(c.subLive, topic)
	c.subLock.Unlock()

	// Notify the carrier of the removal
	c.conn.Unsubscribe(topic)
	return nil
}

// Implements iris.Connection.Close.
func (c *connection) Close() {
	// Signal the connection as terminating
	close(c.term)

	// Remove all topic subscriptions
	c.subLock.Lock()
	for topic, _ := range c.subLive {
		c.conn.Unsubscribe(topic)
	}
	c.subLock.Unlock()

	// Leave the app group
	c.conn.Unsubscribe(appPrefix + c.app)
}
