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

// Contains the wire protocol for the Iris layer communication.

package iris

import (
	"encoding/gob"
	"time"

	"github.com/project-iris/iris/proto"
)

// Iris operation code.
type opcode uint8

const (
	opBcast opcode = iota // Cluster broadcast
	opReq                 // Cluster request
	opRep                 // Cluster reply
	opPub                 // Topic publish
	opTun                 // Tunneling request
)

// Extra headers for the Iris layer.
type header struct {
	Op   opcode // Operation code of the message
	Src  uint64 // Connection id of the sender (requests, tunnel)
	Dest uint64 // Connection id of the recipient (direct messages)

	// Optional fields for requests and replies
	ReqId   uint64        // Request/response identifier
	ReqFail bool          // Flag whether a request failed
	ReqTime time.Duration // Maximum amount of time spendable on the request

	// Optional fields for tunnels
	TunId    uint64        // Id of the tunnel being requested
	TunKey   []byte        // Secret symmetric key of the tunnel
	TunAddrs []string      // Tunnel listener endpoints
	TunTime  time.Duration // Maximum time to establish tunnel
}

// Make sure the header struct is registered with gob.
func init() {
	gob.Register(&header{})
}

// Envelopes an Iris header and payload into the generic packet container.
func (c *Connection) assemblePacket(head *header, data []byte) *proto.Message {
	return &proto.Message{
		Head: proto.Header{
			Meta: head,
		},
		Data: data,
	}
}

// Assembles an application broadcast message. It consists of the bcast opcode
// and the payload.
func (c *Connection) assembleBroadcast(msg []byte) *proto.Message {
	return c.assemblePacket(&header{Op: opBcast}, msg)
}

// Assembles an application request message. It consists of the request opcode,
// the locally unique request id and the payload.
func (c *Connection) assembleRequest(reqId uint64, req []byte, timeout time.Duration) *proto.Message {
	return c.assemblePacket(&header{Op: opReq, Src: c.id, ReqId: reqId, ReqTime: timeout}, req)
}

// Assembles the reply message to an application request. It consists of the
// reply opcode, the original request's id and the payload itself.
func (c *Connection) assembleReply(dest uint64, reqId uint64, rep []byte, err error) *proto.Message {
	if err == nil {
		return c.assemblePacket(&header{Op: opRep, Dest: dest, ReqId: reqId}, rep)
	} else {
		return c.assemblePacket(&header{Op: opRep, Dest: dest, ReqId: reqId, ReqFail: true}, []byte(err.Error()))
	}
}

// Assembles an event message to be published in a topic. It consists of the
// publish opcode and the payload.
func (c *Connection) assemblePublish(msg []byte) *proto.Message {
	return c.assemblePacket(&header{Op: opPub}, msg)
}

// Assembles a tunneling request message, consisting of the tunneling opcode,
// local tunnel id, assigned secret key and reachability infos for the reverse
// stream connection.
func (c *Connection) assembleTunnelRequest(tunId uint64, key []byte, addrs []string, timeout time.Duration) *proto.Message {
	return c.assemblePacket(&header{Op: opTun, Src: c.id, TunId: tunId, TunKey: key, TunAddrs: addrs, TunTime: timeout}, nil)
}
