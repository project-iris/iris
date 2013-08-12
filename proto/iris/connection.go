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
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/pool"
	"github.com/karalabe/iris/proto/carrier"
	"sync"
	"sync/atomic"
	"time"
)

var appPrefixes []string
var topPrefixes []string

// Creates the cluster split prefix tags
func init() {
	appPrefixes = make([]string, config.IrisClusterSplits)
	for i := 0; i < len(appPrefixes); i++ {
		appPrefixes[i] = fmt.Sprintf("app-#%d:", i)
	}
	topPrefixes = make([]string, config.IrisClusterSplits)
	for i := 0; i < len(topPrefixes); i++ {
		topPrefixes[i] = fmt.Sprintf("top-#%d:", i)
	}
}

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

	tunIdx  uint64             // Index to assign the next tunnel
	tunLive map[uint64]*tunnel // Active tunnels
	tunLock sync.RWMutex       // Mutex to protect the tunnel map

	// Quality of service fields
	workers *pool.ThreadPool // Concurrent threads handling the connection
	splitId uint32           // Id of the next prefix for split cluster round-robin

	// Bookkeeping fields
	quit chan chan error // Quit channel to synchronize termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

func Connect(carrier carrier.Carrier, app string, handler ConnectionHandler) Connection {
	// Create the connection object
	c := &connection{
		app:     app,
		handler: handler,

		reqPend: make(map[uint64]chan []byte),
		subLive: make(map[string]SubscriptionHandler),
		tunLive: make(map[uint64]*tunnel),

		// Quality of service
		workers: pool.NewThreadPool(config.IrisHandlerThreads),

		// Bookkeeping
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	c.conn = carrier.Connect(c)
	for _, prefix := range appPrefixes {
		c.conn.Subscribe(prefix + app)
	}
	c.workers.Start()

	return c
}

// Implements iris.Connection.Broadcast. Tags the destination id with the app
// prefix to ensure apps and topics can overlap, bundles the payload into a
// protocol packet and publishes it.
func (c *connection) Broadcast(app string, msg []byte) error {
	prefixIdx := int(atomic.AddUint32(&c.splitId, uint32(1))) % config.IrisClusterSplits
	c.conn.Publish(appPrefixes[prefixIdx]+app, assembleBroadcast(msg))
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
	c.conn.Balance(appPrefixes[int(reqId)%config.IrisClusterSplits]+app, assembleRequest(reqId, req, timeout))

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
	// Make sure there are no double subscriptions and not closing
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return permError(fmt.Errorf("connection terminating"))
	default:
		if _, ok := c.subLive[topPrefixes[0]+topic]; ok {
			c.subLock.Unlock()
			return permError(fmt.Errorf("already subscribed"))
		}
		for _, prefix := range topPrefixes {
			c.subLive[prefix+topic] = handler
		}
	}
	c.subLock.Unlock()

	// Subscribe through the carrier
	for _, prefix := range topPrefixes {
		if err := c.conn.Subscribe(prefix + topic); err != nil {
			return err
		}
	}
	return nil
}

// Implements iris.Connection.Publish. Tags the target with the topic prefix,
// bundles the message and requests the carrier to publish it.
func (c *connection) Publish(topic string, msg []byte) error {
	prefixIdx := int(atomic.AddUint32(&c.splitId, uint32(1))) % config.IrisClusterSplits
	c.conn.Publish(topPrefixes[prefixIdx]+topic, assemblePublish(msg))
	return nil
}

// Implements iris.Connection.Unsubscribe. Removes an active subscription from
// the local list and notifies the carrier of the request.
func (c *connection) Unsubscribe(topic string) error {
	// Remove subscription if present
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return permError(fmt.Errorf("connection terminating"))
	default:
		if _, ok := c.subLive[topPrefixes[0]+topic]; !ok {
			c.subLock.Unlock()
			return permError(fmt.Errorf("not subscribed"))
		}
	}
	for _, prefix := range topPrefixes {
		delete(c.subLive, prefix+topic)
	}
	c.subLock.Unlock()

	// Notify the carrier of the removal
	for _, prefix := range topPrefixes {
		c.conn.Unsubscribe(prefix + topic)
	}
	return nil
}

// Implements iris.Connection.Tunnel.
func (c *connection) Tunnel(app string, timeout time.Duration) (Tunnel, error) {
	c.tunLock.RLock()
	select {
	case <-c.term:
		c.tunLock.RUnlock()
		return nil, permError(fmt.Errorf("connection terminating"))
	default:
		c.tunLock.RUnlock()
		return c.initiateTunnel(app, timeout)
	}
}

// Implements iris.Connection.Close. Removes all topic subscriptions, closes all
// tunnels and disconnects from the carrier.
func (c *connection) Close() {
	// Signal the connection as terminating
	close(c.term)

	// Close all open tunnels
	c.tunLock.Lock()
	for _, tun := range c.tunLive {
		go tun.Close()
	}
	c.tunLock.Unlock()

	// Remove all topic subscriptions
	c.subLock.Lock()
	for topic, _ := range c.subLive {
		c.conn.Unsubscribe(topic)
	}
	c.subLock.Unlock()

	// Leave the app group and close the carrier connection
	for _, prefix := range appPrefixes {
		c.conn.Unsubscribe(prefix + c.app)
	}
	c.conn.Close()

	// Terminate the worker pool
	c.workers.Terminate()
}
