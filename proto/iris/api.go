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

// Package iris contains the framework's public high level API.
package iris

import (
	"time"
)

// Communication interface to the iris framework. Currently the following
// patterns are supported: broadcast, request-reply, publish-subscribe and
// exclusive-pair.
type Connection interface {
	// Executes a synchronous request to app (load balanced between all active),
	// and returns the received reply, or an error if a timeout is reached.
	Request(app string, req []byte, timeout time.Duration) ([]byte, error)

	// Broadcasts asynchronously a message to all applications of type app. No
	// guarantees are made that all nodes receive the message (best effort).
	Broadcast(app string, msg []byte) error

	// Opens a direct tunnel to an instance of app, allowing pairwise-exclusive
	// and order-guaranteed message passing between them. The method blocks until
	// either the newly created tunnel is set up, or a timeout is reached.
	Tunnel(app string, timeout time.Duration) (Tunnel, error)

	// Subscribes to topic, using handler as the callback for arriving events. An
	// error is returned if subscription fails.
	Subscribe(topic string, handler SubscriptionHandler) error

	// Publishes an event asynchronously to topic. No guarantees are made that all
	// subscribers receive the message.
	Publish(topic string, msg []byte) error

	// Unsubscribes from topic, receiving no more event notifications for it.
	Unsubscribe(topic string) error

	// Gracefully terminates the connection and all subscriptions.
	Close()
}

// Handler for the connection scope events: application requests, application
// broadcasts and tunneling requests.
type ConnectionHandler interface {
	// Handles a message broadcast to all applications of the local type.
	HandleBroadcast(msg []byte)

	// Handles the msg request, returning the reply that should be forwarded back
	// to the caller. If the method crashes, nothing is retuned and the caller
	// will eventually time out.
	HandleRequest(req []byte, timeout time.Duration) []byte

	// Handles the request to open a direct tunnel.
	HandleTunnel(tun Tunnel)
}

// Communication stream between the local app and a remote endpoint. Ordered
// message delivery is guaranteed.
type Tunnel interface {
	// Sends an asynchronous message to the remote pair. An error is returned if
	// the operation could not complete.
	Send(msg []byte) error

	// Retrieves a message waiting in the local queue. If none is available, the
	// call blocks until either one arrives or a timeout is reached.
	Recv(timeout time.Duration) ([]byte, error)

	// Closes the tunnel between the pair.
	Close()
}

// Subscription handler receiving events from a single subscribed topic.
type SubscriptionHandler interface {
	// Handles an event published to the subscribed topic.
	HandleEvent(msg []byte)
}
