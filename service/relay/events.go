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

// Event handlers for both relay and carrier side messages. All methods in this
// file are assumed to be running in a separate go routine!

package relay

import (
	"fmt"
	"github.com/karalabe/iris/proto/iris"
	"log"
	"time"
)

// Forwards an app broadcast arriving from the Iris network to the attached app.
// Any error is considered a protocol violation.
func (r *relay) HandleBroadcast(msg []byte) {
	if err := r.sendBroadcast(msg); err != nil {
		log.Printf("relay: broadcast forward error: %v.", err)
		r.drop()
	}
}

// Forwards an app broadcast from the attached relay to the Iris network. Any
// error is considered a protocol violation.
func (r *relay) handleBroadcast(app string, msg []byte) {
	if err := r.iris.Broadcast(app, msg); err != nil {
		log.Printf("relay: broadcast error: %v.", err)
		r.drop()
	}
}

// Forwards a request arriving from the Iris network to the attached app. Also a
// local timer is started to ensure a faulty client doesn't fill the node with
// stale requests. Any error is considered a protocol violation.
func (r *relay) HandleRequest(req []byte, timeout time.Duration) []byte {
	// Create a reply channel for the results
	r.reqLock.Lock()
	reqChan := make(chan []byte, 1)
	reqId := r.reqIdx
	r.reqPend[reqId] = reqChan
	r.reqIdx++
	r.reqLock.Unlock()

	// Ensure no just is left after the function terminates
	defer func() {
		r.reqLock.Lock()
		defer r.reqLock.Unlock()

		delete(r.reqPend, reqId)
		close(reqChan)
	}()
	// Send the request to the specified app
	if err := r.sendRequest(reqId, req); err != nil {
		log.Printf("relay: request error: %v.", err)
		r.drop()
	}
	// Retrieve the results or time out
	select {
	case <-r.term:
		return nil
	case <-time.After(timeout):
		return nil
	case rep := <-reqChan:
		return rep
	}
}

// Forwards a request arriving from the attached app to the Iris network, and
// waits for a reply to arrive back which can be forwarded. If the request times
// out, a reply is sent back accordingly.
func (r *relay) handleRequest(app string, reqId uint64, req []byte, timeout time.Duration) {
	if rep, err := r.iris.Request(app, req, timeout); err != nil {
		r.sendReply(reqId, nil, true)
	} else {
		r.sendReply(reqId, rep, false)
	}
}

// Forwards a reply arriving from the attached app to the Iris node by looking
// up the pending request channel and if still live, inserting the results.
func (r *relay) handleReply(reqId uint64, msg []byte) {
	r.reqLock.RLock()
	defer r.reqLock.RUnlock()

	if ch, ok := r.reqPend[reqId]; ok {
		ch <- msg
	}
}

// Handler for a topic subscription. Forwards all published events to the app
// attached.
type subscriptionHandler struct {
	relay *relay
	topic string
}

// Forwards the arriving event from the Iris network to the attached app. Any
// error is considered a protocol violation.
func (s *subscriptionHandler) HandleEvent(msg []byte) {
	if err := s.relay.sendPublish(s.topic, msg); err != nil {
		log.Printf("relay: publish forward error: %v.", err)
		s.relay.drop()
	}
}

// Forwards a subscription event arriving from the attached app to the Iris node
// and creates a new subscription handler to process the arriving events. Any
// error is considered a protocol violation.
func (r *relay) handleSubscribe(topic string) {
	// Create the event forwarder
	handler := &subscriptionHandler{
		relay: r,
		topic: topic,
	}
	// Subscribe and drop conenction in case of an error
	if err := r.iris.Subscribe(topic, handler); err != nil {
		log.Printf("relay: subscription error: %v.", err)
		r.drop()
	}
}

// Forwards a publish event arriving from the attached app to the Iris node. Any
// error is considered a protocol violation.
func (r *relay) handlePublish(topic string, msg []byte) {
	if err := r.iris.Publish(topic, msg); err != nil {
		log.Printf("relay: publish error: %v.", err)
		r.drop()
	}
}

// Forwards a subscription removel request arriving from the attached app to the
// Iris node. Any error is considered a protocol violation.
func (r *relay) handleUnsubscribe(topic string) {
	if err := r.iris.Unsubscribe(topic); err != nil {
		log.Printf("relay: unsubscription error: %v.", err)
		r.drop()
	}
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
