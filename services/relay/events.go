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

package relay

import (
	"fmt"
	"iris"
	"log"
	"time"
)

func (r *relay) HandleRequest(req []byte, timeout time.Duration) []byte {
	// Create a reply channel for the results
	r.reqLock.Lock()
	reqChan := make(chan []byte, 1)
	reqId := r.reqIdx
	r.reqs[reqId] = reqChan
	r.reqIdx++
	r.reqLock.Unlock()

	// Make sure reply channel is cleaned up
	defer func() {
		r.reqLock.Lock()
		defer r.reqLock.Unlock()
		delete(r.reqs, reqId)
		close(reqChan)
	}()
	// Send the request to the specified app
	if err := r.sendRequest(reqId, req); err != nil {
		return nil
	}
	// Retrieve the results or time out
	tick := time.Tick(timeout)
	select {
	case <-tick:
		return nil
	case rep := <-reqChan:
		return rep
	}
}

// Handles a message broadcast to all applications of the local type.
func (r *relay) HandleBroadcast(msg []byte) {
	r.sendBroadcast(msg)
}

// Handles the request to open a direct tunnel.
func (r *relay) HandleTunnel(tun iris.Tunnel) {
	// Allocate a temporary tunnel id
	r.tunLock.Lock()
	tmpId := r.tunIdx
	tmpCh := make(chan uint64, 1)
	r.tunPend[tmpId] = tmpCh
	r.tunIdx++
	r.tunLock.Unlock()

	// Send a tunneling request to the attached app
	if err := r.sendTunnelRequest(tmpId, 64); err != nil {
		log.Println("FAILED TO TUNNEL")
	}
	// Wait for the final id and save
	tunId := <-tmpCh
	r.tunLock.Lock()
	delete(r.tunPend, tmpId)
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Start some reader/writer whatev
	fmt.Println("START THE IRIS - APP DATA TRANSFERERS")
}

type subscriptionHandler struct {
	r *relay
	t string
}

func (s *subscriptionHandler) HandleEvent(msg []byte) {
	s.r.sendPublish(s.t, msg)
}

// Handles a remote reply by looking up the pending request and forwarding the
// reply.
func (r *relay) handleReply(reqId uint64, msg []byte) {
	r.reqLock.Lock()
	defer r.reqLock.Unlock()

	// Make sure the request is still alive and don't block if dying
	if ch, ok := r.reqs[reqId]; ok {
		ch <- msg
	}
}

func (r *relay) handleTunnelRequest(tunId uint64, app string, timeout time.Duration) {
	// Create the new tunnel connection
	tun, err := r.iris.Tunnel(app, timeout)
	if err != nil {
		fmt.Println("Failed to create iris tunnel")
		return
	}
	// Store the app to iris tunnel
	r.tunLock.Lock()
	r.tunLive[tunId] = tun
	r.tunLock.Unlock()

	// Reply to the app
	r.sendTunnelReply(tunId, 64)

	// Start some reader/writer whatev
	fmt.Println("START THE APP - IRIS DATA TRANSFERERS")
}

func (r *relay) handleTunnelReply(tmpId, tunId uint64) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	if ch, ok := r.tunPend[tmpId]; ok {
		fmt.Println("mapped", tmpId, "to", tunId)
		ch <- tunId
	} else {
		fmt.Println("Temp tunnel already dead")
	}
}

func (r *relay) handleTunnelSend(tunId uint64, msg []byte) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	if tun, ok := r.tunLive[tunId]; ok {
		tun.Send(msg)
	} else {
		fmt.Println("Tunnel dead ? (maybe race, not yet inited)")
	}
}

func (r *relay) handleTunnelRecv(tunId uint64) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	if tun, ok := r.tunLive[tunId]; ok {
		if msg, err := tun.Recv(1000); err == nil {
			r.sendTunnelRecv(tunId, msg)
		} else {
			fmt.Println("shit:", err)
		}
	} else {
		fmt.Println("Tunnel dead ? (maybe race, not yet inited)")
	}
}

func (r *relay) handleTunnelClose(tunId uint64) {
	r.tunLock.Lock()
	defer r.tunLock.Unlock()

	if tun, ok := r.tunLive[tunId]; ok {
		fmt.Println("STOP THE TRANSFER", tun)
		delete(r.tunLive, tunId)
	} else {
		log.Printf("iris: stale close of local tunnel #%v.", tunId)
	}

	fmt.Println("Close res:", r.sendTunnelClose(tunId))
}
