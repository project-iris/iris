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

// Contains the wire protocol for communicating with the Iris bindings.

package relay

import (
	"config"
	"encoding/binary"
	"fmt"
	"iris"
	"log"
	"net"
	"proto/carrier"
	"time"
)

const (
	opBcast byte = iota
	opReq
	opRep
	opSub
	opPub
	opUnsub
	opClose
	opTunReq
	opTunRep
	opTunAck
	opTunData
	opTunPoll
	opTunClose
)

// Accepts an incoming relay connection and logs the app in into the carrier.
func accept(sock net.Conn, car carrier.Carrier) (*relay, error) {
	// Create the relay object
	rel := &relay{
		sock:      sock,
		outVarBuf: make([]byte, binary.MaxVarintLen64),
		inByteBuf: make([]byte, 1),
		inVarBuf:  make([]byte, binary.MaxVarintLen64),
	}
	// Initialize the relay and return
	if err := rel.procInit(car); err != nil {
		return nil, err
	}
	return rel, nil
}

// Serializes a single byte into the relay.
func (r *relay) sendByte(data byte) error {
	if n, err := r.sock.Write([]byte{data}); n != 1 || err != nil {
		return err
	}
	return nil
}

// Serializes a boolean into the relay.
func (r *relay) sendBool(data bool) error {
	if data {
		return r.sendByte(1)
	} else {
		return r.sendByte(0)
	}
}

// Serializes a variable int into the relay.
func (r *relay) sendVarint(data uint64) error {
	size := binary.PutUvarint(r.outVarBuf, data)
	if n, err := r.sock.Write(r.outVarBuf[:size]); n != size || err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged binary array into the relay.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if n, err := r.sock.Write([]byte(data)); n != len(data) || err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay.
func (r *relay) sendString(data string) error {
	if err := r.sendBinary([]byte(data)); err != nil {
		return err
	}
	return nil
}

// Atomically sends a request message into the relay.
func (r *relay) sendRequest(reqId uint64, req []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opReq); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendBinary(req); err != nil {
		return err
	}
	return nil
}

// Atomically sends a reply message into the relay.
func (r *relay) sendReply(reqId uint64, rep []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opRep); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendBinary(rep); err != nil {
		return err
	}
	return nil
}

// Atomically sends an application broadcast message into the relay.
func (r *relay) sendBroadcast(msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	fmt.Println("send bcast", opBcast, msg)

	if err := r.sendByte(opBcast); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic subscription message into the relay.
func (r *relay) sendSubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opSub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendPublish(topic string, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opPub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return nil
}

// Atomically sends a topic unsubscription message into the relay.
func (r *relay) sendUnsubscribe(topic string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opUnsub); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	return nil
}

// Atomically sends a tunneling message into the relay.
func (r *relay) sendTunnelRequest(tunId uint64, win int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunReq); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendVarint(uint64(win))
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendTunnelReply(tunId uint64, win int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunRep); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendVarint(uint64(win))
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendTunnelRecv(tunId uint64, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunData); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendBinary(msg)
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendTunnelClose(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunClose); err != nil {
		return err
	}
	return r.sendVarint(tunId)
}

// Atomically sends a close message into the relay.
func (r *relay) sendClose() error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	return nil
}

// Retrieves a single byte from the relay.
func (r *relay) recvByte() (byte, error) {
	if n, err := r.sock.Read(r.inByteBuf); n != 1 || err != nil {
		return 0, err
	}
	return r.inByteBuf[0], nil
}

// Retrieves a boolean from the relay.
func (r *relay) recvBool() (bool, error) {
	b, err := r.recvByte()
	if err != nil {
		return false, err
	}
	return b == 1, nil
}

// Retrieves a variable int from the relay.
func (r *relay) recvVarint() (uint64, error) {
	// Retrieve the varint bytes one at a time
	index := 0
	for {
		// Retreive the next byte of the varint
		b, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		// Save it and terminate loop if last byte
		r.inVarBuf[index] = b
		index++
		if b&0x80 == 0 {
			break
		}
	}
	if num, n := binary.Uvarint(r.inVarBuf[:index]); n <= 0 {
		return 0, fmt.Errorf("failed to decode varint %v", r.inVarBuf[:index])
	} else {
		return num, nil
	}
}

// Retrieves a length-tagged binary array from the relay.
func (r *relay) recvBinary() ([]byte, error) {
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	if n, err := r.sock.Read(data); n != int(size) || err != nil {
		return nil, err
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), err
	}
}

// Retrieves the connection initialization and processes it.
func (r *relay) procInit(car carrier.Carrier) error {
	// Read and verify API compatibility
	ver, err := r.recvString()
	if err != nil {
		return err
	}
	if ver != config.RelayVersion {
		return fmt.Errorf("incompatible version: have %v, want %v", ver, config.RelayVersion)
	}
	app, err := r.recvString()
	if err != nil {
		return err
	}
	r.iris = iris.Connect(car, app, r)
	return nil
}

// Retrieves a remote request from the relay and processes it.
func (r *relay) procRequest() error {
	// Retrieve the message parts
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	app, err := r.recvString()
	if err != nil {
		return err
	}
	req, err := r.recvBinary()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go func() {
		if rep, err := r.iris.Request(app, req, time.Duration(timeout)*time.Millisecond); err == nil {
			r.sendReply(reqId, rep)
		}
	}()
	return nil
}

// Retrieves a remote reply from the relay and processes it.
func (r *relay) procReply() error {
	// Retrieve the message parts
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	rep, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the reply to the pending handler routine
	go r.handleReply(reqId, rep)
	return nil
}

// Retrieves a remote broadcast message from the relay and processes it.
func (r *relay) procBroadcast() error {
	// Retrieve the message parts
	app, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.iris.Broadcast(app, msg)
	return nil
}

func (r *relay) procSubscribe() error {
	// Retrieve the message parts
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.iris.Subscribe(topic, &subscriptionHandler{r, topic})
	return nil
}

func (r *relay) procPublish() error {
	// Retrieve the message parts
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.iris.Publish(topic, msg)
	return nil

}

func (r *relay) procUnsubscribe() error {
	// Retrieve the message parts
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	// Pass the request to the iris connection
	go r.iris.Unsubscribe(topic)
	return nil
}

// Retrieves a tunneling request and relays it.
func (r *relay) procTunnelRequest() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	app, err := r.recvString()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Pass the tunnel request to the iris connection
	go r.handleTunnelRequest(tunId, app, time.Duration(timeout)*time.Millisecond)
	return nil
}

// Retrieves a tunneling reply and relays it.
func (r *relay) procTunnelReply() error {
	// Retrieve the message parts
	peerId, err := r.recvVarint()
	if err != nil {
		return err
	}
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Pass the tunnel request to the iris connection
	go r.handleTunnelReply(peerId, tunId)
	return nil
}

// Retrieves a tunnel message relays it.
func (r *relay) procTunnelData() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	// Pass the tunnel request to the iris connection
	go r.handleTunnelSend(tunId, msg)
	return nil
}

func (r *relay) procTunnelPoll() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Pass the tunnel request to the iris connection
	go r.handleTunnelRecv(tunId)
	return nil
}

// Retrieves a tunneling request and relays it.
func (r *relay) procTunnelClose() error {
	// Retrieve the message parts
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	// Pass the tunnel request to the iris connection
	go r.handleTunnelClose(tunId)
	return nil
}

// Retrieves messages from the client connection and keeps processing them until
// either side closes the socket.
func (r *relay) process() {
	defer func() {
		r.iris.Close()
		r.sock.Close()
	}()

	var op byte
	var err error
	for err == nil {
		// Retrieve the next message opcode
		if op, err = r.recvByte(); err == nil {
			// Read the rest of the message and process
			switch op {
			case opBcast:
				err = r.procBroadcast()
			case opReq:
				err = r.procRequest()
			case opRep:
				err = r.procReply()
			case opSub:
				err = r.procSubscribe()
			case opPub:
				err = r.procPublish()
			case opUnsub:
				err = r.procUnsubscribe()
			case opTunReq:
				err = r.procTunnelRequest()
			case opTunRep:
				err = r.procTunnelReply()
			case opTunData:
				err = r.procTunnelData()
			case opTunPoll:
				err = r.procTunnelPoll()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				r.sendClose()
				return
			default:
				err = fmt.Errorf("unknown operation code: %v", op)
			}
		}
	}
	// Log the error and terminate
	log.Printf("relay failure: %v", err)
}
