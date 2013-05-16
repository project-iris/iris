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
	"iris"
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
