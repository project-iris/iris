// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

// Contains the wire protocol for communicating with an Iris binding.

// The specification version implemented is v1.0-draft2, available at:
// http://iris.karalabe.com/specs/relay-protocol-v1.0-draft2.pdf

package relay

import (
	"fmt"
	"io"
	"time"
)

// Packet opcodes
const (
	opInit  byte = 0x00 // In: connection initiation           | Out: connection acceptance
	opDeny       = 0x01 // In: <never received                 | Out: connection refusal
	opClose      = 0x02 // In: connection tear-down initiation | Out: connection tear-down notification

	opBroadcast = 0x03 // In: application broadcast initiation | Out: application broadcast delivery
	opRequest   = 0x04 // In: application request initiation   | Out: application request delivery
	opReply     = 0x05 // In: application reply initiation     | Out: application reply delivery

	opSubscribe   = 0x06 // In: topic subscription             | Out: <never sent>
	opUnsubscribe = 0x07 // In: topic subscription removal     | Out: <never sent>
	opPublish     = 0x08 // In: topic event publish            | Out: topic event delivery

	opTunInit     = 0x09 // In: tunnel construction request    | Out: tunnel initiation
	opTunConfirm  = 0x0a // In: tunnel confirmation            | Out: tunnel construction result
	opTunAllow    = 0x0b // In: tunnel transfer allowance      | Out: <same as out>
	opTunTransfer = 0x0c // In: tunnel data exchange           | Out: <same as out>
	opTunClose    = 0x0d // In: tunnel termination request     | Out: tunnel termination notification
)

// Protocol constants
var (
	protoVersion = "v1.0-draft2"
	clientMagic  = "iris-client-magic"
	relayMagic   = "iris-relay-magic"
)

// Serializes a single byte into the relay connection.
func (r *relay) sendByte(data byte) error {
	return r.sockBuf.WriteByte(data)
}

// Serializes a boolean into the relay connection.
func (r *relay) sendBool(data bool) error {
	if data {
		return r.sendByte(1)
	}
	return r.sendByte(0)
}

// Serializes a variable int into the relay using base 128 encoding into the relay connection.
func (r *relay) sendVarint(data uint64) error {
	for data > 127 {
		// Internal byte, set the continuation flag and send
		if err := r.sendByte(byte(128 + data%128)); err != nil {
			return err
		}
		data /= 128
	}
	// Final byte, send and return
	return r.sendByte(byte(data))
}

// Serializes a length-tagged binary array into the relay connection.
func (r *relay) sendBinary(data []byte) error {
	if err := r.sendVarint(uint64(len(data))); err != nil {
		return err
	}
	if _, err := r.sockBuf.Write([]byte(data)); err != nil {
		return err
	}
	return nil
}

// Serializes a length-tagged string into the relay connection.
func (r *relay) sendString(data string) error {
	return r.sendBinary([]byte(data))
}

// Sends a connection acceptance.
func (r *relay) sendInit() error {
	if err := r.sendByte(opInit); err != nil {
		return err
	}
	if err := r.sendString(relayMagic); err != nil {
		return err
	}
	if err := r.sendString(protoVersion); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a connection denial.
func (r *relay) sendDeny(reason string) error {
	if err := r.sendByte(opDeny); err != nil {
		return err
	}
	if err := r.sendString(relayMagic); err != nil {
		return err
	}
	if err := r.sendString(reason); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends a connection tear-down notification.
func (r *relay) sendClose(reason string) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opClose); err != nil {
		return err
	}
	if err := r.sendString(reason); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Sends an application broadcast delivery.
func (r *relay) sendBroadcast(message []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opBroadcast); err != nil {
		return err
	}
	if err := r.sendBinary(message); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Atomically sends a request message into the relay.
func (r *relay) sendRequest(reqId uint64, req []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opRequest); err != nil {
		return err
	}
	if err := r.sendVarint(reqId); err != nil {
		return err
	}
	if err := r.sendBinary(req); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Atomically sends a reply message into the relay.
func (r *relay) sendReply(reqId uint64, rep []byte, timeout bool) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opReply); err != nil {
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
	return r.sockBuf.Flush()
}

// Atomically sends a topic publish message into the relay.
func (r *relay) sendPublish(topic string, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opPublish); err != nil {
		return err
	}
	if err := r.sendString(topic); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Atomically sends a tunneling message into the relay.
func (r *relay) sendTunnelRequest(tmpId uint64, buf int) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunInit); err != nil {
		return err
	}
	if err := r.sendVarint(tmpId); err != nil {
		return err
	}
	if err := r.sendVarint(uint64(buf)); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Atomically sends a tunneling reply into the relay.
func (r *relay) sendTunnelReply(tunId uint64, buf int, timeout bool) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunConfirm); err != nil {
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
	return r.sockBuf.Flush()
}

// Atomically sends a tunnel data packet into the relay.
func (r *relay) sendTunnelData(tunId uint64, msg []byte) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunTransfer); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	if err := r.sendBinary(msg); err != nil {
		return err
	}
	return r.sockBuf.Flush()
}

// Atomically sends a tunnel data acknowledgement into the relay.
func (r *relay) sendTunnelAck(tunId uint64) error {
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	if err := r.sendByte(opTunAllow); err != nil {
		return err
	}
	if err := r.sendVarint(tunId); err != nil {
		return err
	}
	return r.sockBuf.Flush()
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
	return r.sockBuf.Flush()
}

// Retrieves a single byte from the relay connection.
func (r *relay) recvByte() (byte, error) {
	return r.sockBuf.ReadByte()
}

// Retrieves a boolean from the relay connection.
func (r *relay) recvBool() (bool, error) {
	b, err := r.recvByte()
	if err != nil {
		return false, err
	}
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("protocol violation: invalid boolean value: %v", b)
	}
}

// Retrieves a variable int from the relay in base 128 encoding from the relay connection.
func (r *relay) recvVarint() (uint64, error) {
	var num uint64
	for i := uint(0); ; i++ {
		chunk, err := r.recvByte()
		if err != nil {
			return 0, err
		}
		num += uint64(chunk&127) << (7 * i)
		if chunk <= 127 {
			break
		}
	}
	return num, nil
}

// Retrieves a length-tagged binary array from the relay connection.
func (r *relay) recvBinary() ([]byte, error) {
	// Fetch the length of the binary blob
	size, err := r.recvVarint()
	if err != nil {
		return nil, err
	}
	// Fetch the blob itself
	data := make([]byte, size)
	if _, err := io.ReadFull(r.sockBuf, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Retrieves a length-tagged string from the relay connection.
func (r *relay) recvString() (string, error) {
	if data, err := r.recvBinary(); err != nil {
		return "", err
	} else {
		return string(data), nil
	}
}

// Retrieves a connection initiation request.
func (r *relay) procInit() (string, string, error) {
	// Retrieve the init code
	if op, err := r.recvByte(); err != nil {
		return "", "", err
	} else if op != opInit {
		return "", "", fmt.Errorf("protocol violation: invalid init code: %v.", op)
	}
	// Retrieve and check the client side magic
	if magic, err := r.recvString(); err != nil {
		return "", "", err
	} else if magic != clientMagic {
		return "", "", fmt.Errorf("protocol violation: invalid client magic: %s", magic)
	}
	// Retrieve the protocol version
	version, err := r.recvString()
	if err != nil {
		return "", "", err
	}
	// Retrieve the cluster id
	cluster, err := r.recvString()
	if err != nil {
		return "", "", err
	}
	return version, cluster, nil
}

// Retrieves a connection tear-down initiation.
func (r *relay) procClose() error {
	// The packet is empty beside the opcode
	return nil
}

// Retrieves an application broadcast initiation.
func (r *relay) procBroadcast() error {
	cluster, err := r.recvString()
	if err != nil {
		return err
	}
	message, err := r.recvBinary()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleBroadcast(cluster, message) })
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
	r.workers.Schedule(func() { r.handleReply(reqId, rep) })
	return nil
}

// Retrieves a subscription request and forwards it to the Iris network.
func (r *relay) procSubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleSubscribe(topic) })
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
	r.workers.Schedule(func() { r.handlePublish(topic, msg) })
	return nil

}

// Retrieves a subscription removal event and forwards it to the Iris netowrk.
func (r *relay) procUnsubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleUnsubscribe(topic) })
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
	r.workers.Schedule(func() { r.handleTunnelRequest(tunId, app, int(buf), time.Duration(timeout)*time.Millisecond) })
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
	r.workers.Schedule(func() { r.handleTunnelReply(tmpId, tunId, int(buf)) })
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
	r.handleTunnelSend(tunId, msg) // Note, NOT separate go-routine, need to preserve order a bit longer
	return nil
}

// Retrieves a tunnel data acknowledgement and processes it.
func (r *relay) procTunnelAck() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleTunnelAck(tunId) })
	return nil
}

// Retrieves a tunneling request and relays it.
func (r *relay) procTunnelClose() error {
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleTunnelClose(tunId, true) })
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
			case opBroadcast:
				err = r.procBroadcast()
			case opRequest:
				err = r.procRequest()
			case opReply:
				err = r.procReply()
			case opSubscribe:
				err = r.procSubscribe()
			case opUnsubscribe:
				err = r.procUnsubscribe()
			case opPublish:
				err = r.procPublish()
			case opTunInit:
				err = r.procTunnelRequest()
			case opTunConfirm:
				err = r.procTunnelReply()
			case opTunAllow:
				err = r.procTunnelAck()
			case opTunTransfer:
				err = r.procTunnelData()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				if err = r.procClose(); err == nil {
					if err = r.sendClose(""); err == nil {
						closed = true
					}
				}
			default:
				err = fmt.Errorf("protocol violation: unknown opcode: %v", op)
			}
		}
	}
	// Failure or deliberate close, clean up resources
	r.sock.Close()
	r.iris.Close()
	r.workers.Terminate(true)

	// Signal termination to all blocked threads
	close(r.term)

	// Notify the supervisor and report error if any
	r.done <- r
	errc := <-r.quit
	errc <- err
}
