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

// Contains the wire protocol for communicating with an Iris binding.

// The specification version implemented is v1.0-draft2, available at:
// http://iris.karalabe.com/specs/relay-protocol-v1.0-draft2.pdf

package relay

import (
	"fmt"
	"io"
	"sync/atomic"
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

// Serializes a packet through a closure into the relay connection.
func (r *relay) sendPacket(closure func() error) error {
	// Increment the pending write count
	atomic.AddInt32(&r.sockWait, 1)

	// Acquire the socket lock
	r.sockLock.Lock()
	defer r.sockLock.Unlock()

	// Send the packet itself
	if err := closure(); err != nil {
		// Decrement the pending count and error out
		atomic.AddInt32(&r.sockWait, -1)
		return err
	}
	// Flush the stream if no more messages are pending
	if atomic.AddInt32(&r.sockWait, -1) == 0 {
		return r.sockBuf.Flush()
	}
	return nil
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
	return r.sendPacket(func() error {
		if err := r.sendByte(opClose); err != nil {
			return err
		}
		return r.sendString(reason)
	})
}

// Sends an application broadcast delivery.
func (r *relay) sendBroadcast(message []byte) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opBroadcast); err != nil {
			return err
		}
		return r.sendBinary(message)
	})
}

// Sends an application request delivery.
func (r *relay) sendRequest(id uint64, request []byte, timeout int) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opRequest); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		if err := r.sendBinary(request); err != nil {
			return err
		}
		return r.sendVarint(uint64(timeout))
	})
}

// Sends an application reply delivery.
func (r *relay) sendReply(id uint64, reply []byte, fault string) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opReply); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		timeout := (reply == nil && len(fault) == 0)
		if err := r.sendBool(timeout); err != nil {
			return err
		}
		if timeout {
			return nil
		}
		success := (len(fault) == 0)
		if err := r.sendBool(success); err != nil {
			return err
		}
		if success {
			return r.sendBinary(reply)
		} else {
			return r.sendString(fault)
		}
	})
}

// Sends a topic event delivery.
func (r *relay) sendPublish(topic string, event []byte) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opPublish); err != nil {
			return err
		}
		if err := r.sendString(topic); err != nil {
			return err
		}
		return r.sendBinary(event)
	})
}

// Sends a tunnel initiation.
func (r *relay) sendTunnelInit(id uint64, chunkLimit int) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opTunInit); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		return r.sendVarint(uint64(chunkLimit))
	})
}

// Sends a tunnel construction result.
func (r *relay) sendTunnelResult(id uint64, chunkLimit int) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opTunConfirm); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		timeout := (chunkLimit == 0)
		if err := r.sendBool(timeout); err != nil {
			return err
		}
		if timeout {
			return nil
		}
		return r.sendVarint(uint64(chunkLimit))
	})
}

// Sends a tunnel allowance message.
func (r *relay) sendTunnelAllowance(id uint64, space int) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opTunAllow); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		return r.sendVarint(uint64(space))
	})
}

// Sends a tunnel data exchange message.
func (r *relay) sendTunnelTransfer(id uint64, size int, payload []byte) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opTunTransfer); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		if err := r.sendVarint(uint64(size)); err != nil {
			return err
		}
		return r.sendBinary(payload)
	})
}

// Atomically sends a tunnel close request into the relay.
func (r *relay) sendTunnelClose(id uint64, reason string) error {
	return r.sendPacket(func() error {
		if err := r.sendByte(opTunClose); err != nil {
			return err
		}
		if err := r.sendVarint(id); err != nil {
			return err
		}
		return r.sendString(reason)
	})
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

// Retrieves an application request initiation.
func (r *relay) procRequest() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	cluster, err := r.recvString()
	if err != nil {
		return err
	}
	request, err := r.recvBinary()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	go r.handleRequest(cluster, id, request, time.Duration(timeout)*time.Millisecond)
	return nil
}

// Retrieves an application reply delivery.
func (r *relay) procReply() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	success, err := r.recvBool()
	if err != nil {
		return err
	}

	var reply []byte
	var fault string
	if success {
		if reply, err = r.recvBinary(); err != nil {
			return err
		}
	} else {
		if fault, err = r.recvString(); err != nil {
			return err
		}
	}
	r.workers.Schedule(func() { r.handleReply(id, reply, fault) })
	return nil
}

// Retrieves a topic subscription.
func (r *relay) procSubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleSubscribe(topic) })
	return nil
}

// Retrieves a topic subscription removal.
func (r *relay) procUnsubscribe() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleUnsubscribe(topic) })
	return nil
}

// Retrieves a topic event publish.
func (r *relay) procPublish() error {
	topic, err := r.recvString()
	if err != nil {
		return err
	}
	event, err := r.recvBinary()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handlePublish(topic, event) })
	return nil
}

// Retrieves a tunnel construction request.
func (r *relay) procTunnelInit() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	cluster, err := r.recvString()
	if err != nil {
		return err
	}
	timeout, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleTunnelInit(id, cluster, time.Duration(timeout)*time.Millisecond) })
	return nil
}

// Retrieves a tunnel confirmation.
func (r *relay) procTunnelConfirm() error {
	buildId, err := r.recvVarint()
	if err != nil {
		return err
	}
	tunId, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.handleTunnelConfirm(buildId, tunId) // Finalize tunnel state before accepting further messages
	return nil
}

// Retrieves a tunnel allowance message.
func (r *relay) procTunnelAllowance() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	space, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleTunnelAllowance(id, int(space)) })
	return nil
}

// Retrieves a tunnel data message and relays it.
func (r *relay) procTunnelTranfer() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	size, err := r.recvVarint()
	if err != nil {
		return err
	}
	payload, err := r.recvBinary()
	if err != nil {
		return err
	}
	r.handleTunnelSend(id, int(size), payload)
	return nil
}

// Retrieves a tunnel close request.
func (r *relay) procTunnelClose() error {
	id, err := r.recvVarint()
	if err != nil {
		return err
	}
	r.workers.Schedule(func() { r.handleTunnelClose(id, true, "") })
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
				err = r.procTunnelInit()
			case opTunConfirm:
				err = r.procTunnelConfirm()
			case opTunAllow:
				err = r.procTunnelAllowance()
			case opTunTransfer:
				err = r.procTunnelTranfer()
			case opTunClose:
				err = r.procTunnelClose()
			case opClose:
				if err = r.procClose(); err == nil {
					// Graceful close, unregister from Iris and wait for pending ops
					r.iris.Unregister()
					r.workers.Terminate(false)

					// Notify the binding of the closure, then Iris
					if err = r.sendClose(""); err == nil {
						closed = true
					}
				}
			default:
				err = fmt.Errorf("protocol violation: unknown opcode: %v", op)
			}
		}
	}
	// If an error occurred, force stop execution
	if err != nil {
		r.workers.Terminate(true)
	}
	// Close both Iris and relay connections
	r.iris.Close()
	r.sock.Close()

	// Signal termination to all blocked threads
	close(r.term)

	// Notify the supervisor and report error if any
	r.done <- r
	errc := <-r.quit
	errc <- err
}
