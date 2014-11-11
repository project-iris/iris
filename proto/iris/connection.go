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

package iris

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/pool"
)

// Iris specific errors
var ErrTerminating = errors.New("terminating")
var ErrTimeout = errors.New("timeout")
var ErrSubscribed = errors.New("already subscribed")
var ErrNotSubscribed = errors.New("not subscribed")

// Prefixes for multi-clustering.
var clusterPrefixes []string
var topicPrefixes []string

// Creates the cluster split prefix tags.
func init() {
	clusterPrefixes = make([]string, config.IrisClusterSplits)
	for i := 0; i < len(clusterPrefixes); i++ {
		clusterPrefixes[i] = fmt.Sprintf("c#%d-", i)
	}
	topicPrefixes = make([]string, config.IrisClusterSplits)
	for i := 0; i < len(topicPrefixes); i++ {
		topicPrefixes[i] = fmt.Sprintf("t#%d-", i)
	}
}

// Handler for the connection scope events: application requests, application
// broadcasts and tunneling requests.
type ConnectionHandler interface {
	// Handles a message broadcast to all applications of the local type.
	HandleBroadcast(msg []byte)

	// Handles the request, returning the reply that should be forwarded back to
	// the caller. If the method crashes, nothing is returned and the caller will
	// eventually time out.
	HandleRequest(req []byte, timeout time.Duration) ([]byte, error)

	// Handles the request to open a direct tunnel.
	HandleTunnel(tun *Tunnel)
}

// Subscription handler receiving events from a single subscribed topic.
type SubscriptionHandler interface {
	// Handles an event published to the subscribed topic.
	HandleEvent(msg []byte)
}

// Connection through which to interact with other iris clients.
type Connection struct {
	// Application layer fields
	id      uint64            // Auto-incremented connection id
	cluster string            // Cluster to which the client registers
	handler ConnectionHandler // Handler for connection events
	iris    *Overlay          // Interface into the distributed carrier

	reqIdx  uint64                 // Index to assign the next request
	reqReps map[uint64]chan []byte // Reply channels for active requests
	reqErrs map[uint64]chan error  // Error channels for active requests
	reqLock sync.RWMutex           // Mutex to protect the result channel maps

	subLive map[string]SubscriptionHandler // Active subscriptions
	subLock sync.RWMutex                   // Mutex to protect the subscription map

	tunIdx  uint64             // Index to assign the next tunnel
	tunLive map[uint64]*Tunnel // Tunnels either live, or being established
	tunLock sync.RWMutex       // Mutex to protect the tunnel map

	// Quality of service fields
	workers *pool.ThreadPool // Concurrent threads handling the connection
	splitId uint32           // Id of the next prefix for split cluster round-robin

	// Bookkeeping fields
	quit chan chan error // Quit channel to synchronize termination
	term chan struct{}   // Channel to signal termination to blocked go-routines
}

// Connects to the iris overlay. The parameters can be either both specified, in
// the case of a service registration, or both skipped in the case of a client
// connection. Others combinations will fail.
func (o *Overlay) Connect(cluster string, handler ConnectionHandler) (*Connection, error) {
	// Make sure only valid argument combinations pass
	if (cluster == "" && handler != nil) || (cluster != "" && handler == nil) {
		return nil, fmt.Errorf("invalid connection arguments: cluster '%v', handler %v", cluster, handler)
	}
	// Create the connection object
	c := &Connection{
		cluster: cluster,
		handler: handler,
		iris:    o,

		reqReps: make(map[uint64]chan []byte),
		reqErrs: make(map[uint64]chan error),
		subLive: make(map[string]SubscriptionHandler),
		tunLive: make(map[uint64]*Tunnel),

		// Quality of service
		workers: pool.NewThreadPool(config.IrisHandlerThreads),

		// Bookkeeping
		quit: make(chan chan error),
		term: make(chan struct{}),
	}
	// Assign a connection id and track it
	o.lock.Lock()
	c.id, o.autoid = o.autoid, o.autoid+1
	o.conns[c.id] = c
	o.lock.Unlock()

	// Subscribe to the multi-group if the connection is a service
	if c.cluster != "" {
		for _, prefix := range clusterPrefixes {
			if err := c.iris.subscribe(c.id, prefix+cluster); err != nil {
				return nil, err
			}
		}
	}
	c.workers.Start()

	return c, nil
}

// Broadcasts asynchronously a message to all members of an iris cluster. No
// guarantees are made that all nodes receive the message (best effort).
func (c *Connection) Broadcast(cluster string, msg []byte) error {
	prefixIdx := int(atomic.AddUint32(&c.splitId, 1)) % config.IrisClusterSplits
	return c.iris.scribe.Publish(clusterPrefixes[prefixIdx]+cluster, c.assembleBroadcast(msg))
}

// Executes a synchronous request to cluster (load balanced between all active),
// and returns the received reply, or an error if a timeout is reached.
func (c *Connection) Request(cluster string, req []byte, timeout time.Duration) ([]byte, error) {
	// Create a reply and error channel for the results
	repc := make(chan []byte, 1)
	errc := make(chan error, 1)

	c.reqLock.Lock()
	reqId := c.reqIdx
	c.reqIdx++
	c.reqReps[reqId] = repc
	c.reqErrs[reqId] = errc
	c.reqLock.Unlock()

	// Make sure the result channels are cleaned up
	defer func() {
		c.reqLock.Lock()
		delete(c.reqReps, reqId)
		delete(c.reqErrs, reqId)
		close(repc)
		close(errc)
		c.reqLock.Unlock()
	}()
	// Send the request
	prefixIdx := int(reqId) % config.IrisClusterSplits
	c.iris.scribe.Balance(clusterPrefixes[prefixIdx]+cluster, c.assembleRequest(reqId, req, timeout))

	// Retrieve the results, time out or fail if terminating
	select {
	case <-c.term:
		return nil, ErrTerminating
	case <-time.After(timeout):
		return nil, ErrTimeout
	case reply := <-repc:
		return reply, nil
	case err := <-errc:
		return nil, err
	}
}

// Subscribes to topic, using handler as the callback for arriving events. An
// error is returned if subscription fails.
func (c *Connection) Subscribe(topic string, handler SubscriptionHandler) error {
	// Make sure there are no double subscriptions and not closing
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return ErrTerminating
	default:
		if _, ok := c.subLive[topicPrefixes[0]+topic]; ok {
			c.subLock.Unlock()
			return ErrSubscribed
		}
		for _, prefix := range topicPrefixes {
			c.subLive[prefix+topic] = handler
		}
	}
	c.subLock.Unlock()

	// Subscribe through the carrier
	for _, prefix := range topicPrefixes {
		if err := c.iris.subscribe(c.id, prefix+topic); err != nil {
			return err
		}
	}
	return nil
}

// Publishes an event asynchronously to topic. No guarantees are made that all
// subscribers receive the message.
func (c *Connection) Publish(topic string, msg []byte) error {
	prefixIdx := int(atomic.AddUint32(&c.splitId, 1)) % config.IrisClusterSplits
	return c.iris.scribe.Publish(topicPrefixes[prefixIdx]+topic, c.assemblePublish(msg))
}

// Unsubscribes from topic, receiving no more event notifications for it.
func (c *Connection) Unsubscribe(topic string) error {
	// Remove subscription if present
	c.subLock.Lock()
	select {
	case <-c.term:
		c.subLock.Unlock()
		return ErrTerminating
	default:
		if _, ok := c.subLive[topicPrefixes[0]+topic]; !ok {
			c.subLock.Unlock()
			return ErrNotSubscribed
		}
	}
	for _, prefix := range topicPrefixes {
		delete(c.subLive, prefix+topic)
	}
	c.subLock.Unlock()

	// Notify the carrier of the removal
	for _, prefix := range topicPrefixes {
		if err := c.iris.unsubscribe(c.id, prefix+topic); err != nil {
			return err
		}
	}
	return nil
}

// Opens a direct tunnel to a member of cluster, allowing pairwise-exclusive
// and order-guaranteed message passing between them. The method blocks until
// either the newly created tunnel is set up, or a timeout is reached.
func (c *Connection) Tunnel(cluster string, timeout time.Duration) (*Tunnel, error) {
	c.tunLock.RLock()
	select {
	case <-c.term:
		c.tunLock.RUnlock()
		return nil, ErrTerminating
	default:
		c.tunLock.RUnlock()
		return c.initiateTunnel(cluster, timeout)
	}
}

// Closes the service aspect of the connection, but leave the client alive.
func (c *Connection) Unregister() error {
	if c.cluster != "" {
		// Remove the cluster subscriptions
		for _, prefix := range clusterPrefixes {
			c.iris.unsubscribe(c.id, prefix+c.cluster)
		}
		// Make sure the service is marked unregistered
		c.cluster = ""
	}
	return nil
}

// Gracefully terminates the connection, all subscriptions and all tunnels.
func (c *Connection) Close() error {
	// Signal the connection as terminating
	close(c.term)

	// Close all open tunnels
	c.tunLock.Lock()
	closing := new(sync.WaitGroup)
	for _, tun := range c.tunLive {
		closing.Add(1)
		go func() {
			defer closing.Done()
			tun.Close()
		}()
	}
	c.tunLock.Unlock()
	closing.Wait()

	// Remove all topic subscriptions
	c.subLock.Lock()
	for topic, _ := range c.subLive {
		c.iris.unsubscribe(c.id, topic)
	}
	c.subLock.Unlock()

	// Leave the cluster if it was a service connection
	if err := c.Unregister(); err != nil {
		return err
	}
	// Terminate the worker pool
	c.workers.Terminate(true)

	// Drop the connection from the tracked list
	c.iris.lock.Lock()
	delete(c.iris.conns, c.id)
	c.iris.lock.Unlock()

	return nil
}
