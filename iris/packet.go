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

// This file contains the various methods for assembling iris packets.

package iris

import (
	"encoding/gob"
	"proto/session"
)

// Iris operation code type.
type opcode uint8

// Iris operation types.
const (
	opReq opcode = iota
	opRep
	opBcast
	opPub
	opTunReq
	opTunRep
	opTunDat
)

// Extra headers for the iris implementation.
type header struct {
	Op    opcode  // Operation code of the message
	ReqId *uint64 // Request identifier (req or tunnel)
	RepId *uint64 // Responce identifier (tunnel or message)
}

// Make sure the header struct is registered with gob.
func init() {
	gob.Register(&header{})
}

// Assembles a request packet.
func assembleRequest(id uint64, msg []byte) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:    opReq,
				ReqId: &id,
			},
		},
		Data: msg,
	}
}

// Assembles a reply packet.
func assembleReply(id uint64, msg []byte) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:    opRep,
				ReqId: &id,
			},
		},
		Data: msg,
	}
}

// Assembles an application broadcast packet.
func assembleBroadcast(msg []byte) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op: opBcast,
			},
		},
		Data: msg,
	}
}

// Assembles a topic publish packet.
func assemblePublish(msg []byte) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op: opPub,
			},
		},
		Data: msg,
	}
}

// Assembles a tunneling request packet.
func assembleTunnelRequest(id uint64) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:    opTunReq,
				ReqId: &id,
			},
		},
	}
}

// Assembles a tunneling reply packet.
func assembleTunnelReply(peerId, localId uint64) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:    opTunRep,
				ReqId: &peerId,
				RepId: &localId,
			},
		},
	}
}

// Assembles a tunneling reply packet.
func assembleTunnelData(tunId uint64, msg []byte) *session.Message {
	return &session.Message{
		Head: session.Header{
			Meta: &header{
				Op:    opTunDat,
				ReqId: &tunId,
			},
		},
		Data: msg,
	}
}
