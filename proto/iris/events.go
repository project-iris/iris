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

// Event handlers mostly for the carrier side messages.

package iris

import (
	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/carrier"
	"log"
	"time"
)

// Implements proto.carrier.ConnectionCallback.HandleDirect. Extracts the data
// from the Iris envelope and calls the appropriate handler. All handlers should
// be invoked in a new go-routine.
func (c *connection) HandleDirect(src *carrier.Address, msg *proto.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opRep:
		c.workers.Schedule(func() { c.handleReply(head.ReqId, msg.Data) })
	case opTunRep:
		c.workers.Schedule(func() { c.handleTunnelReply(src, head.TunId, head.TunRemId) })
	case opTunData:
		c.workers.Schedule(func() { c.handleTunnelData(head.TunId, head.TunSeqId, msg.Data) })
	case opTunAck:
		c.workers.Schedule(func() { c.handleTunnelAck(head.TunId, head.TunSeqId) })
	case opTunGrant:
		c.workers.Schedule(func() { c.handleTunnelGrant(head.TunId, head.TunSeqId) })
	case opTunClose:
		c.workers.Schedule(func() { c.handleTunnelClose(head.TunId) })
	default:
		log.Printf("iris: invalid direct opcode: %v.", head.Op)
	}
}

// Implements proto.carrier.ConnectionCallback.HandlePublish. Extracts the data
// from the Iris envelope and calls the appropriate handler. All handlers should
// be invoked in a new go-routine.
func (c *connection) HandlePublish(src *carrier.Address, topic string, msg *proto.Message) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opBcast:
		c.workers.Schedule(func() { c.handleBroadcast(msg.Data) })
	case opReq:
		c.workers.Schedule(func() { c.handleRequest(src, head.ReqId, msg.Data, head.ReqTime) })
	case opPub:
		c.workers.Schedule(func() { c.handlePublish(topic, msg.Data) })
	case opTunReq:
		c.workers.Schedule(func() { c.handleTunnelRequest(src, head.TunRemId) })
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

// Delivers a topic event to a subscribed handler. If the subscription does not
// exist the message is silently dropped.
func (c *connection) handlePublish(topic string, msg []byte) {
	// Fetch the handler
	c.subLock.RLock()
	handler, ok := c.subLive[topic]
	c.subLock.RUnlock()

	// Deliver the event
	if ok {
		handler.HandleEvent(msg)
	}
}

// Accepts the inbound tunnel, notifies the remote endpoint of the success and
// starts the local handler.
func (c *connection) handleTunnelRequest(peerAddr *carrier.Address, peerId uint64) {
	if tun, err := c.acceptTunnel(peerAddr, peerId); err != nil {
		// This should not happend, potential extension point to re-route tunnel request
		panic(err)
	} else {
		c.handler.HandleTunnel(tun)
	}
}

// Initiates a locally outbound pending tunnel with the remote endpoint data and
// notifies the tunneler.
func (c *connection) handleTunnelReply(peerAddr *carrier.Address, tunId uint64, peerId uint64) {
	c.tunLock.Lock()
	defer c.tunLock.Unlock()

	// Make sure the tunnel is still pending and initialize it if so
	if tun, ok := c.tunLive[tunId]; ok {
		tun.peerAddr = peerAddr
		tun.peerId = peerId
		tun.init <- struct{}{}
	}
}

// Delivers tunnel trafic from the Iris network to the specific tunnel.
func (c *connection) handleTunnelData(tunId uint64, seqId uint64, msg []byte) {
	// Fetch the target tunnel
	c.tunLock.RLock()
	tun, ok := c.tunLive[tunId]
	c.tunLock.RUnlock()

	// Deliver the message
	if ok {
		tun.handleData(seqId, msg)
	}
}

// Delivers a tunnel data acknowledgement from the Iris network to the specific
// local endpoint.
func (c *connection) handleTunnelAck(tunId uint64, seqId uint64) {
	// Fetch the target tunnel
	c.tunLock.RLock()
	tun, ok := c.tunLive[tunId]
	c.tunLock.RUnlock()

	// Deliver the ack
	if ok {
		tun.handleAck(seqId)
	}
}

// Delivers a tunnel data allowance grant from the Iris network to the specific
// local endpoint.
func (c *connection) handleTunnelGrant(tunId uint64, seqId uint64) {
	// Fetch the target tunnel
	c.tunLock.RLock()
	tun, ok := c.tunLive[tunId]
	c.tunLock.RUnlock()

	// Deliver the allowance
	if ok {
		tun.handleGrant(seqId)
	}
}

// Handles the local or remote closure of the tunnel by terminating all internal
// operations and removing it from the set of live tunnels.
func (c *connection) handleTunnelClose(tunId uint64) {
	c.tunLock.Lock()
	defer c.tunLock.Unlock()

	// Make sure the tunnel is still live
	if tun, ok := c.tunLive[tunId]; ok {
		close(tun.term)
		delete(c.tunLive, tunId)
	}
}
