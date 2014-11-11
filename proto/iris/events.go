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

// Event handlers mostly for the carrier side messages.

package iris

import (
	"errors"
	"log"
	"math/big"
	"math/rand"
	"time"

	"github.com/project-iris/iris/proto"
)

// Implements proto.iris.ConnectionCallback.HandlePublish. Extracts the data from
// the Iris envelope and calls the appropriate handler.
func (o *Overlay) HandlePublish(src *big.Int, topic string, msg *proto.Message) {
	head := msg.Head.Meta.(*header)

	// Fetch the message recipients
	o.lock.RLock()
	subs, ok := o.subLive[topic]
	if !ok {
		o.lock.RUnlock()
		log.Printf("iris: non-existent topic: %v.", topic)
		return
	}
	conns := make([]*Connection, len(subs))
	for i, id := range subs {
		conns[i] = o.conns[id]
	}
	o.lock.RUnlock()

	// Publish to every live subscription
	for i := 0; i < len(conns); i++ {
		conn := conns[i] // Closure
		switch head.Op {
		case opBcast:
			conn.workers.Schedule(func() { conn.handleBroadcast(msg.Data) })
		case opPub:
			conn.workers.Schedule(func() { conn.handlePublish(topic, msg.Data) })
		default:
			log.Printf("iris: invalid publish opcode: %v.", head.Op)
		}
	}
}

// Implements proto.iris.ConnectionCallback.HandlePublish. Extracts the data from
// the Iris envelope and calls the appropriate handler.
func (o *Overlay) HandleBalance(src *big.Int, topic string, msg *proto.Message) {
	head := msg.Head.Meta.(*header)

	// Fetch the possible message recipients and pick one at random
	o.lock.RLock()
	subs, ok := o.subLive[topic]
	if !ok {
		o.lock.RUnlock()
		log.Printf("iris: non-existent topic: %v.", topic)
		return
	}
	conn := o.conns[subs[rand.Intn(len(subs))]]
	o.lock.RUnlock()

	// Balance to the chose one
	switch head.Op {
	case opReq:
		conn.workers.Schedule(func() { conn.handleRequest(src, head.Src, head.ReqId, msg.Data, head.ReqTime) })
	case opTun:
		conn.workers.Schedule(func() { conn.handleTunnelRequest(head.Src, head.TunId, head.TunKey, head.TunAddrs, head.TunTime) })
	default:
		log.Printf("iris: invalid balance opcode: %v.", head.Op)
	}
}

// Implements proto.scribe.ConnectionCallback.HandleDirect. Extracts the data
// from the Iris envelope and calls the appropriate handler.
func (o *Overlay) HandleDirect(src *big.Int, msg *proto.Message) {
	head := msg.Head.Meta.(*header)

	// Fetch the intended recipient
	o.lock.RLock()
	conn, ok := o.conns[head.Dest]
	o.lock.RUnlock()
	if !ok {
		log.Printf("iris: non-existent direct recipient: %v", head.Dest)
		return
	}
	// Pass the message to the connection to handle
	switch head.Op {
	case opRep:
		conn.workers.Schedule(func() { conn.handleReply(head.ReqId, head.ReqFail, msg.Data) })
	default:
		log.Printf("iris: invalid direct opcode: %v.", head.Op)
	}
}

// Passes the broadcast message up to the application handler.
func (c *Connection) handleBroadcast(msg []byte) {
	c.handler.HandleBroadcast(msg)
}

// Passes the request up to the application handler, also specifying the timeout
// under which the reply must be sent back. Either a reply or a binding side
// failure is forwarded to the remote node.
func (c *Connection) handleRequest(srcNode *big.Int, srcConn uint64, reqId uint64, msg []byte, timeout time.Duration) {
	rep, err := c.handler.HandleRequest(msg, timeout)
	if err == ErrTerminating || err == ErrTimeout {
		return
	}
	c.iris.scribe.Direct(srcNode, c.assembleReply(srcConn, reqId, rep, err))
}

// Looks up the result channel for the pending request and inserts the reply. If
// the channel doesn't exist any more the reply is silently dropped.
func (c *Connection) handleReply(reqId uint64, failed bool, data []byte) {
	c.reqLock.RLock()
	defer c.reqLock.RUnlock()

	// Interpret the data as either a reply or a failure string
	if !failed {
		if repc, ok := c.reqReps[reqId]; ok {
			repc <- data
		}
	} else {
		if errc, ok := c.reqErrs[reqId]; ok {
			errc <- errors.New(string(data))
		}
	}
}

// Delivers a topic event to a subscribed handler. If the subscription does not
// exist the message is silently dropped.
func (c *Connection) handlePublish(topic string, msg []byte) {
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
func (c *Connection) handleTunnelRequest(conn uint64, id uint64, key []byte, addrs []string, timeout time.Duration) {
	// Validate the remote address list
	if len(addrs) == 0 {
		log.Printf("iris: empty address list for tunnel request.")
		return
	}
	// Try to establish the outbound tunnel
	if tun, err := c.buildTunnel(conn, id, key, addrs, timeout); err != nil {
		log.Printf("iris: failed to accept tunnel: %v.", err)
	} else {
		c.handler.HandleTunnel(tun)
	}
}

// Removes a closing tunnel from the list and returns whether it existed or not.
// This flag is required to prevent simultaneous double closes.
func (c *Connection) handleTunnelClose(id uint64) bool {
	c.tunLock.Lock()
	defer c.tunLock.Unlock()

	// Make sure the tunnel has not been removed yet
	if _, ok := c.tunLive[id]; !ok {
		return false
	}
	// Remove the tunnel from the active list
	delete(c.tunLive, id)
	return true
}
