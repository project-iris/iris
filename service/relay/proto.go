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
	"fmt"
	"time"
)

const (
	opInit byte = iota
	opBcast
	opReq
	opRep
	opSub
	opPub
	opUnsub
	opClose
	opTunReq
	opTunRep
	opTunData
	opTunAck
	opTunClose
)

// Relay protocol version
var relayVersion = "v1.0"

// Serializes a single byte into the relay.
func (r *relay) sendByte(data byte) error {
	if err := r.sockBuf.WriteByte(data); err != nil {
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
	for {
		if data > 127 {
			// Internal byte, set the continuation flag and send
			if err := r.sendByte(byte(128 + data%128)); err != nil {
				return err
			}
			data /= 128
		} else {
			// Final byte, send and return
			return r.sendByte(byte(data))
		}
	}
}

// Serializes a length-tagged binary array into the relay.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if n, err := r.sockBuf.Write([]byte(data)); n != len(data) || err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay.
func (r *relay) sendString(data string) error {
	return r.sendBinary([]byte(data))
}

// Flushes the output buffer into the network stream.
func (r *relay) sendFlush() error {
	if err := r.sockBuf.Flush(); err != nil {
		return err
	}
	return nil
}

// Serializes the initialization confirmation.
func (r *relay) sendInit() error {
	if err := r.sendByte(opInit); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends an application broadcast message into the relay.
func (r *relay) sendBroadcast(msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opBcast); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sendFlush()
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
	return r.sendFlush()
}

// Atomically sends a reply message into the relay.
func (r *relay) sendReply(reqId uint64, rep []byte, timeout bool) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opRep); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendBool(timeout); err != nil {
		return err
	}
	if !timeout {
		if err := r.sendBinary(rep); err != nil {
			return err
		}
	}
	return r.sendFlush()
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
	return r.sendFlush()
}

// Atomically sends a close message into the relay.
func (r *relay) sendClose() error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunneling message into the relay.
func (r *relay) sendTunnelRequest(tmpId uint64, buf int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunReq); err != nil {
		return err
	}
	if err := r.sendVarint(tmpId); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(buf)); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunneling reply into the relay.
func (r *relay) sendTunnelReply(tunId uint64, buf int, timeout bool) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunRep); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendBool(timeout); err != nil {
		return err
	}
	if !timeout {
		if err := r.sendVarint(uint64(buf)); err != nil {
			return err
		}
	}
	return r.sendFlush()
}

// Atomically sends a tunnel data packet into the relay.
func (r *relay) sendTunnelData(tunId uint64, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunData); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunnel data acknowledgement into the relay.
func (r *relay) sendTunnelAck(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunAck); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendFlush()
}

// Atomically sends a tunnel close request into the relay.
func (r *relay) sendTunnelClose(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunClose); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sendFlush()
}

// Retrieves a single byte from the relay.
func (r *relay) recvByte() (byte, error) {
	b, err := r.sockBuf.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
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
	var num uint64
	for i := uint(0); ; i++ {
		// Retreive the next byte of the varint
		b, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		// Save it and terminate if last byte
		if b > 127 {
			num += uint64(b-128) << (7 * i)
		} else {
			num += uint64(b) << (7 * i)
			break
		}
	}
	return num, nil
}

// Retrieves a length-tagged binary array from the relay.
func (r *relay) recvBinary() ([]byte, error) {
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	data := make([]byte, size)
	read := uint64(0)
	for read < size {
		if n, err := r.sockBuf.Read(data[read:]); err != nil {
			return nil, err
		} else {
			read += uint64(n)
		}
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Retrieves the connection initialization and processes it.
func (r *relay) procInit() (string, error) {
	// Retrieve the init code
	if op, err := r.recvByte(); err != nil {
		return "", err
	} else if op != opInit {
		return "", fmt.Errorf("relay: protocol violation: invalid init code: %v.", op)
	}
	// Retrieve and check the protocol version
	if ver, err := r.recvString(); err != nil {
		return "", err
	} else if ver != relayVersion {
		return "", fmt.Errorf("relay: protocol violation: incompatible version: have %v, want %v", ver, relayVersion)
	}
	// Retrieve the app id
	app, err := r.recvString()
	if err != nil {
		return "", err
	}
	return app, nil
}

// Retrieves a local broadcast message from the relay and forwards to the Iris network.
func (r *relay) procBroadcast() error {
	app, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleBroadcast(app, msg)
	return nil
}

// Retrieves a local request from the relay and forwards to the Iris network.
func (r *relay) procRequest() error {
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
	go r.handleRequest(app, reqId, req, time.Duration(timeout)*time.Millisecond)
	return nil
}

// Retrieves a local reply from the relay and forwards to the Iris network.
func (r *relay) procReply() error {
	reqId, err := r.recvVarint()
	if err != nil {
		return err
	}
	rep, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleReply(reqId, rep)
	return nil
}

// Retrieves a subscription request and forwards it to the Iris network.
func (r *relay) procSubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	go r.handleSubscribe(topic)
	return nil
}

// Retrieves a publish request and forwards it to the Iris network.
func (r *relay) procPublish() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handlePublish(topic, msg)
	return nil

}

// Retrieves a subscription removal event and forwards it to the Iris netowrk.
func (r *relay) procUnsubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	go r.handleUnsubscribe(topic)
	return nil
}

// Retrieves a tunneling request and forwards it to the Iris network.
func (r *relay) procTunnelRequest() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	app, err := r.recvString()
	if err != nil {
		return err
	}
	buf, err := r.recvVarint()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelRequest(tunId, app, int(buf), time.Duration(timeout)*time.Millisecond)
	return nil
}

// Retrieves a tunneling reply and finalizes the tunnel building.
func (r *relay) procTunnelReply() error {
	tmpId, err := r.recvVarint()
	if err != nil {
		return err
	}
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	buf, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelReply(tmpId, tunId, int(buf))
	return nil
}

// Retrieves a tunnel data message and relays it.
func (r *relay) procTunnelData() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	msg, err := r.recvBinary()
	if err != nil {
		return err
	}
	go r.handleTunnelSend(tunId, msg)
	return nil
}

// Retrieves a tunnel data acknowledgement and processes it.
func (r *relay) procTunnelAck() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelAck(tunId)
	return nil
}

// Retrieves a tunneling request and relays it.
func (r *relay) procTunnelClose() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleTunnelClose(tunId, true)
	return nil
}

// Retrieves messages from the client connection and keeps processing them until
// either side closes the socket or the connection drops.
func (r *relay) process() {
	var op byte
	var err error
	for closed := false; !closed && err == nil; {
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
			case opTunAck:
				err = r.procTunnelAck()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				err = r.sendClose()
				closed = true
			default:
				err = fmt.Errorf("unknown opcode: %v", op)
			}
		}
	}
	// Failure or deliberate close, clean up resources
	r.sock.Close()
	r.iris.Close()

	// Signal termination to all blocked threads
	close(r.term)

	// Notify the supervisor and report error if any
	r.done <- r
	errc := <-r.quit
	errc <- err
}
