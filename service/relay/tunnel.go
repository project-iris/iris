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

package relay

import (
	"fmt"
	"log"
	"sync"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/container/queue"
	"github.com/project-iris/iris/proto/iris"
)

// Relay tunnel wrapping the real Iris tunnel, adding input and output buffers.
type tunnel struct {
	id  uint64       // Local-app tunnel identifier
	tun *iris.Tunnel // Iris tunnel being relayed and buffered
	rel *relay       // Message relay to the attached app

	// Quality of service fields
	atoiSize *queue.Queue  // Iris to application size buffer
	atoiData *queue.Queue  // Iris to application message buffer
	atoiSign chan struct{} // Allowance grant signaler
	atoiLock sync.Mutex    // Protects the allowance and signaler

	itoaSpace int           // Iris to attached application space allowance
	itoaSign  chan struct{} // Allowance grant signaler
	itoaLock  sync.Mutex    // Protects the allowance and signaler

	// Bookkeeping fields
	quit chan chan error // Quit channel to synchronize tunnel termination
}

// Creates a new relay tunnel and associated buffers.
func (r *relay) newTunnel(id uint64, tun *iris.Tunnel) *tunnel {
	return &tunnel{
		id:  id,
		tun: tun,
		rel: r,

		atoiSize: queue.New(),
		atoiData: queue.New(),
		atoiSign: make(chan struct{}, 1),
		itoaSign: make(chan struct{}, 1),

		quit: make(chan chan error),
	}
}

// Acknowledges a received packet allowing further sends.
func (t *tunnel) grantAllowance(space int) {
	t.itoaLock.Lock()
	defer t.itoaLock.Unlock()

	t.itoaSpace += space
	select {
	case t.itoaSign <- struct{}{}:
	default:
	}
}

// Buffers a binding message chunk to be sent to the remote endpoint.
func (t *tunnel) sendChunk(size int, payload []byte) error {
	// Make sure the chunk limit is not violated
	if len(payload) > config.RelayTunnelChunkLimit {
		return fmt.Errorf("chunk limit exceeded: %d > %d", len(payload), config.RelayTunnelChunkLimit)
	}

	t.atoiLock.Lock()
	defer t.atoiLock.Unlock()

	t.atoiSize.Push(size)
	t.atoiData.Push(payload)

	select {
	case t.atoiSign <- struct{}{}:
	default:
	}
	return nil
}

// Closes the tunnel and stops any data transfer.
func (t *tunnel) close() {
	// Terminate the two transfer routines
	for i := 0; i < 2; i++ {
		errc := make(chan error)
		t.quit <- errc
		if err := <-errc; err != nil {
			// Common for closed tunnels, don't fill log with junk
			// log.Printf("relay: tunnel failure (%d): %v", i+1, err)
		}
	}
}

// Forwards messages arriving from the attached binding to the Iris network.
func (t *tunnel) sender() {
	var err error
	var errc chan error

	// Loop until termination is requested
	for errc == nil && err == nil {
		// Short circuit if a message is available
		if size, chunk := t.fetchMessage(); chunk != nil {
			err = t.tun.Send(size, chunk)
			continue
		}
		// Otherwise wait for availability signal
		select {
		case errc = <-t.quit:
			// Closing
		case <-t.atoiSign:
			// Potentially available message, retry
			continue
		}
	}
	// Sync termination and send back any error
	if errc == nil {
		errc = <-t.quit
	}
	errc <- err
}

// Fetches the next buffered message, or nil if none is available. If a message
// was available, grants the remote side the space allowance just consumed.
func (t *tunnel) fetchMessage() (int, []byte) {
	t.atoiLock.Lock()
	defer t.atoiLock.Unlock()

	if !t.atoiData.Empty() {
		size := t.atoiSize.Pop().(int)
		data := t.atoiData.Pop().([]byte)
		t.rel.workers.Schedule(func() {
			if err := t.rel.sendTunnelAllowance(t.id, len(data)); err != nil {
				log.Printf("relay: send ack failed: %v.", err)
				t.rel.drop()
			}
		})
		return size, data
	}
	// No message, reset arrival flag
	select {
	case <-t.atoiSign:
	default:
	}
	return 0, nil
}

// Forwards messages arriving from the Iris network to the attached application.
func (t *tunnel) receiver() {
	var err error
	var errc chan error

	// Loop until termination is requested
	size, left, chunk, rerr := 0, 0, []byte(nil), error(nil)
	for errc == nil && err == nil {
		// Fetch a message to deliver if none pending
		if chunk == nil {
			size, chunk, rerr = t.tun.Recv(config.RelayTunnelPoll)
			if rerr != nil && rerr != iris.ErrTimeout {
				// Report a terminated tunnel
				reason := rerr.Error()
				if rerr == iris.ErrTerminating {
					reason = ""
				}
				go t.rel.handleTunnelClose(t.id, false, reason)

				// Break out of the receiver loop
				err = rerr
				continue
			}
		}
		// If no message could be retrieved, check quit flag and loop
		if chunk == nil {
			select {
			case errc = <-t.quit:
			default:
			}
			continue
		}
		// If we have a chunk, loop until deliverable
		for errc == nil && err == nil {
			// If the previous message has not completed, force send
			force := false
			switch {
			case size == 0:
				force, left = true, left-len(chunk)
			case left != 0:
				force, left = true, size-len(chunk)
			default:
				left = size - len(chunk)
			}
			if t.drainAllowance(len(chunk), force) {
				err = t.rel.sendTunnelTransfer(t.id, size, chunk)
				size, chunk = 0, nil
				break
			}
			// Wait for a potential allowance grant
			select {
			case errc = <-t.quit:
			case <-t.itoaSign:
			}
		}
	}
	// Sync termination and send back any error
	if errc == nil {
		errc = <-t.quit
	}
	errc <- err
}

// Checks whether there is enough space allowance available to send a message.
// If yes, the allowance is reduced accordingly. Chunks are forcefully send over
// since a large enough message would clog up the link.
func (t *tunnel) drainAllowance(need int, force bool) bool {
	t.itoaLock.Lock()
	defer t.itoaLock.Unlock()

	if force || t.itoaSpace >= need {
		t.itoaSpace -= need
		return true
	}
	// Not enough, reset allowance grant flag
	select {
	case <-t.itoaSign:
	default:
	}
	return false
}
