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

package relay

import (
	"fmt"
	"log"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto/iris"
)

// Relay tunnel wrapping the real Iris tunnel, adding input and output buffers.
type tunnel struct {
	id  uint64       // Local-app tunnel identifier
	tun *iris.Tunnel // Iris tunnel being relayed and buffered
	rel *relay       // Message relay to the attached app

	// Throttling fields
	atoi chan []byte   // Application to Iris message buffer
	itoa chan struct{} // Iris to application pending ack buffer

	// Bookkeeping fields
	quit chan chan error // Quit channel to synchronize tunnel termination
}

// Creates a new relay tunnel and associated buffers.
func (r *relay) newTunnel(id uint64, tun *iris.Tunnel, atoiBuf int, itoaBuf int) *tunnel {
	return &tunnel{
		id:   id,
		tun:  tun,
		rel:  r,
		atoi: make(chan []byte, atoiBuf),
		itoa: make(chan struct{}, itoaBuf),
		quit: make(chan chan error),
	}
}

// Buffers an app message to be sent to the remote endpoint. Fails in case of a
// filled buffer.
func (t *tunnel) send(msg []byte) error {
	select {
	case t.atoi <- msg:
		return nil
	default:
		return fmt.Errorf("buffer full")
	}
}

// Acknowledges a received packet allowing further sends.
func (t *tunnel) ack() error {
	select {
	case <-t.itoa:
		return nil
	default:
		return fmt.Errorf("ack out of bounds")
	}
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

// Forwards messages arriving from the application to the Iris network.
func (t *tunnel) sender() {
	var err error
	var errc chan error

	// Loop until termination is requested
	for errc == nil && err == nil {
		select {
		case errc = <-t.quit:
			// Closing
		case msg := <-t.atoi:
			// Send the message and ack the client (async)
			if err = t.tun.Send(msg); err == nil {
				go func() {
					if err := t.rel.sendTunnelAck(t.id); err != nil {
						log.Printf("relay: send ack failed: %v.", err)
						t.rel.drop()
					}
				}()
			}
		}
	}
	// Sync termination and send back any error
	if errc == nil {
		errc = <-t.quit
	}
	errc <- err
}

// Forwards messages arriving from the Iris network to the attached application.
func (t *tunnel) receiver() {
	var err error
	var errc chan error

	// Loop until termination is requested
	for errc == nil && err == nil {
		select {
		case errc = <-t.quit:
			// Closing
		case t.itoa <- struct{}{}:
			// Message send permitted
			if msg, rerr := t.tun.Recv(time.Duration(config.RelayTunnelPoll) * time.Millisecond); rerr == nil {
				t.rel.handleTunnelRecv(t.id, msg)
			} else if rerr == iris.ErrTimeout {
				<-t.itoa
			} else {
				go t.rel.handleTunnelClose(t.id, false)
				err = rerr
			}
		}
	}
	// Sync termination and send back any error
	if errc == nil {
		errc = <-t.quit
	}
	errc <- err
}
