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
	"github.com/karalabe/iris/proto/session"
	"log"
	"time"
)

// Implements proto.carrier.ConnectionCallback.HandleDirect.
func (c *connection) HandleDirect(src *carrier.Address, msg *session.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opRep:
		c.handleReply(*head.ReqId, msg.Data)
	case opTunRep:
		c.handleTunnelReply(src, *head.ReqId, *head.RepId)
	case opTunDat:
		c.handleTunnelData(*head.ReqId, msg.Data)
	default:
		log.Printf("unsupported opcode in direct handler: %v.", head.Op)
	}
}

// Implements proto.carrier.ConnectionCallback.HandlePublish.
func (c *connection) HandlePublish(src *carrier.Address, topic string, msg *session.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opReq:
		c.handleRequest(src, *head.ReqId, msg.Data)
	case opBcast:
		c.handleBroadcast(msg.Data)
	case opPub:
		c.handlePublish(topic, msg.Data)
	case opTunReq:
		c.handleTunnelRequest(src, *head.ReqId)
	default:
		log.Printf("unsupported opcode in publish handler: %v.", head.Op)
	}
}

// Handles a remote request by calling the app layer handler on a new thread and
// replying to the source node with the result.
func (c *connection) handleRequest(src *carrier.Address, reqId uint64, msg []byte) {
	go func() {
		// HACK, FIX TIME SECOND
		res := c.hand.HandleRequest(msg, time.Second)
		c.relay.Direct(src, assembleReply(reqId, res))
	}()
}

// Handles a remote reply by looking up the pending request and forwarding the
// reply.
func (c *connection) handleReply(reqId uint64, msg []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the request is still alive and don't block if dying
	if ch, ok := c.reqs[reqId]; ok {
		ch <- msg
	}
}

// Handles an application broadcast event.
func (c *connection) handleBroadcast(msg []byte) {
	go c.hand.HandleBroadcast(msg)
}

// Handles a remote topic publish event.
func (c *connection) handlePublish(topic string, msg []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if hand, ok := c.subs[topic]; ok {
		go hand.HandleEvent(msg)
	}
}

// Handles a remote tunneling event.
func (c *connection) handleTunnelRequest(src *carrier.Address, peerId uint64) {
	if tun, err := c.acceptTunnel(src, peerId); err != nil {
		fmt.Println("Tunnel request error: ", err)
	} else {
		go c.hand.HandleTunnel(tun)
	}
}

// Handles teh response to a local tunneling request.
func (c *connection) handleTunnelReply(src *carrier.Address, localId uint64, peerId uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fmt.Println("Handling tunnel reply")

	// Make sure the tunnel is still pending and initialize it if so
	if tun, ok := c.tuns[localId]; ok {
		tun.peerAddr = src
		tun.init <- peerId
	}
}

// Handles teh tunnel data traffic.
func (c *connection) handleTunnelData(tunId uint64, msg []byte) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fmt.Println("Handling tunnel data", tunId, msg)

	// Make sure the tunnel is still live
	if tun, ok := c.tuns[tunId]; ok {
		tun.handleData(msg)
	}
}
