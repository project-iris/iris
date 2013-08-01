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

// Contains the wire protocol for the Iris layer communication.

package iris

import (
	"encoding/gob"
	"github.com/karalabe/iris/proto"
	"time"
)

// Iris operation code.
type opcode uint8

const (
	opBcast    opcode = iota // Application broadcast
	opReq                    // Application request
	opRep                    // Application reply
	opPub                    // Topic publish
	opTunReq                 // Tunnel building request
	opTunRep                 // Tunnel building reply
	opTunData                // Tunnel data transfer
	opTunAck                 // Tunnel data acknowledgement
	opTunGrant               // Tunnel data flow allowance
	opTunClose               // Tunnel closing
)

// Extra headers for the Iris layer.
type header struct {
	Op opcode // Operation code of the message

	// Optional fields for requests and replies
	ReqId   uint64        // Request/response identifier
	ReqTime time.Duration // Maximum amount of time spendable on the request

	// Optional fields for tunnels
	TunId    uint64 // Destination tunnel
	TunRemId uint64 // Remote tunnel endpoint, used during initiation
	TunSeqId uint64 // Sequence number of the message during data transfer
}

// Make sure the header struct is registered with gob.
func init() {
	gob.Register(&header{})
}

// Envelopes an Iris header and payload into the generic packet container.
func assemblePacket(head *header, data []byte) *proto.Message {
	return &proto.Message{
		Head: proto.Header{
			Meta: head,
		},
		Data: data,
	}
}

// Assembles an application broadcast message. It consists of the bcast opcode
// and the payload.
func assembleBroadcast(msg []byte) *proto.Message {
	return assemblePacket(&header{Op: opBcast}, msg)
}

// Assembles an application request message. It consists of the request opcode,
// the locally unique request id and the payload.
func assembleRequest(reqId uint64, req []byte, timeout time.Duration) *proto.Message {
	return assemblePacket(&header{Op: opReq, ReqId: reqId, ReqTime: timeout}, req)
}

// Assembles the reply message to an application request. It consists of the
// reply opcode, the original request's id and the payload itself.
func assembleReply(reqId uint64, rep []byte) *proto.Message {
	return assemblePacket(&header{Op: opRep, ReqId: reqId}, rep)
}

// Assembles an event message to be published in a topic. It consists of the
// publish opcode and the payload.
func assemblePublish(msg []byte) *proto.Message {
	return assemblePacket(&header{Op: opPub}, msg)
}

// Assembles a tunneling request message, consisting of the tunneling opcode and
// the local tunnel id.
func assembleTunnelRequest(tunId uint64) *proto.Message {
	return assemblePacket(&header{Op: opTunReq, TunRemId: tunId}, nil)
}

// Assembles a tunneling reply message, consisting of the tunnel reply opcode
// and the two tunnel endpoint ids.
func assembleTunnelReply(tunId, repTunId uint64) *proto.Message {
	return assemblePacket(&header{Op: opTunRep, TunId: tunId, TunRemId: repTunId}, nil)
}

// Assembles a tunnel data packet, consisting of the data opcode, the tunnel id,
// the message sequence number and the payload itself.
func assembleTunnelData(tunId uint64, seqId uint64, msg []byte) *proto.Message {
	return assemblePacket(&header{Op: opTunData, TunId: tunId, TunSeqId: seqId}, msg)
}

// Assembles a tunnel ack packet, consisting of the ack opcode, the tunnel id
// and the message sequence number.
func assembleTunnelAck(tunId uint64, seqId uint64) *proto.Message {
	return assemblePacket(&header{Op: opTunAck, TunId: tunId, TunSeqId: seqId}, nil)
}

// Assembles a tunnel data allowance packet, consisting of the grant opcode, the
// tunnel id and the sequence number.
func assembleTunnelGrant(tunId uint64, seqId uint64) *proto.Message {
	return assemblePacket(&header{Op: opTunGrant, TunId: tunId, TunSeqId: seqId}, nil)
}

// Assembles a tunnel closure message, consisting of the opcode and the target
// tunnel id.
func assembleTunnelClose(tunId uint64) *proto.Message {
	return assemblePacket(&header{Op: opTunClose, TunId: tunId}, nil)
}
