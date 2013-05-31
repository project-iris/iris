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

// Event handlers for carrier side messages.

package iris

import (
	"fmt"
	"github.com/karalabe/iris/proto/carrier"
	"github.com/karalabe/iris/proto/session"
	"log"
	"time"
)

// Implements proto.carrier.ConnectionCallback.HandleDirect. Extracts the data
// from the Iris envelope and calls the appropriate handler. All handlers should
// be invoked in a new go-routine.
func (c *connection) HandleDirect(src *carrier.Address, msg *session.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opRep:
		go c.handleReply(*head.ReqId, msg.Data)
	case opTunRep:
		go c.handleTunnelReply(src, *head.ReqId, *head.RepId)
	case opTunDat:
		go c.handleTunnelData(*head.ReqId, msg.Data)
	case opTunClose:
		go c.handleTunnelClose(*head.ReqId)
	default:
		log.Printf("iris: invalid direct opcode: %v.", head.Op)
	}
}

// Implements proto.carrier.ConnectionCallback.HandlePublish. Extracts the data
// from the Iris envelope and calls the appropriate handler. All handlers should
// be invoked in a new go-routine.
func (c *connection) HandlePublish(src *carrier.Address, topic string, msg *session.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opBcast:
		go c.handleBroadcast(msg.Data)
	case opReq:
		go c.handleRequest(src, *head.ReqId, msg.Data, *head.ReqTime)
	case opPub:
		go c.handlePublish(topic, msg.Data)
	case opTunReq:
		go c.handleTunnelRequest(src, *head.ReqId)
	default:
		log.Printf("iris: invalid publish opcode: %v.", head.Op)
	}
}

// Passes the broadcast message up to the application handler.
func (c *connection) handleBroadcast(msg []byte) {
	c.handler.HandleBroadcast(msg)
}

// Passes the request up to the application handler, also specifying the timeout
// under which the reply must be sent back. Only a non-nil reply is forwarded to
// the requester.
func (c *connection) handleRequest(src *carrier.Address, reqId uint64, msg []byte, timeout time.Duration) {
	if rep := c.handler.HandleRequest(msg, timeout); rep != nil {
		c.conn.Direct(src, assembleReply(reqId, rep))
	}
}

// Looks up the result channel for the pending request and inserts the reply. If
// the channel doesn't exist any more the reply is silently dropped.
func (c *connection) handleReply(reqId uint64, rep []byte) {
	c.reqLock.RLock()
	defer c.reqLock.RUnlock()

	// Make sure the request is still alive and don't block if dying
	if ch, ok := c.reqPend[reqId]; ok {
		ch <- rep
	}
}

// Handles a remote topic publish event.
func (c *connection) handlePublish(topic string, msg []byte) {
	c.subLock.RLock()
	defer c.subLock.RUnlock()

	if handler, ok := c.subLive[topic]; ok {
		go handler.HandleEvent(msg)
	}
}

// Handles a remote tunneling event.
func (c *connection) handleTunnelRequest(src *carrier.Address, peerId uint64) {
	if tun, err := c.acceptTunnel(src, peerId); err != nil {
		fmt.Println("Tunnel request error: ", err)
	} else {
		go c.handler.HandleTunnel(tun)
	}
}

// Handles teh response to a local tunneling request.
func (c *connection) handleTunnelReply(src *carrier.Address, localId uint64, peerId uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

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

	// Make sure the tunnel is still live
	if tun, ok := c.tuns[tunId]; ok {
		tun.handleData(msg)
	}
}

func (c *connection) handleTunnelClose(tunId uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the tunnel is still live
	if tun, ok := c.tuns[tunId]; ok {
		close(tun.term)
	}
}
