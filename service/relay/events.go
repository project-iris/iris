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

// Event handlers for both relay and carrier side messages. Almost all methods
// in this file are assumed to be running in a separate go routine! The only two
// exceptions are the tunnel data transfers, which need total ordering.

package relay

import (
	"errors"
	"log"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto/iris"
)

// Forwards a broadcast arriving from the Iris network to the attached binding.
func (r *relay) HandleBroadcast(msg []byte) {
	if err := r.sendBroadcast(msg); err != nil {
		log.Printf("relay: broadcast forward error: %v.", err)
		r.drop()
	}
}

// Forwards a broadcast from the attached binding to the Iris network.
func (r *relay) handleBroadcast(app string, msg []byte) {
	if err := r.iris.Broadcast(app, msg); err != nil {
		log.Printf("relay: broadcast error: %v.", err)
		r.drop()
	}
}

// Forwards a request arriving from the Iris network to the attached binding. A
// local timer is started to ensure a faulty client doesn't fill the node with
// stale requests.
func (r *relay) HandleRequest(request []byte, timeout time.Duration) ([]byte, error) {
	// Create a reply and error channel for the results
	repc := make(chan []byte, 1)
	errc := make(chan error, 1)

	r.reqLock.Lock()
	reqId := r.reqIdx
	r.reqIdx++
	r.reqReps[reqId] = repc
	r.reqErrs[reqId] = errc
	r.reqLock.Unlock()

	// Make sure the result channels are cleaned up
	defer func() {
		r.reqLock.Lock()
		delete(r.reqReps, reqId)
		delete(r.reqErrs, reqId)
		close(repc)
		close(errc)
		r.reqLock.Unlock()
	}()
	// Send the request
	if err := r.sendRequest(reqId, request, int(timeout.Nanoseconds()/1000000)); err != nil {
		log.Printf("relay: request error: %v.", err)
		r.drop()
		return nil, err
	}
	// Retrieve the results or fail if terminating
	select {
	case <-r.term:
		return nil, iris.ErrTerminating
	case <-time.After(timeout):
		return nil, iris.ErrTimeout
	case reply := <-repc:
		return reply, nil
	case err := <-errc:
		return nil, err
	}
}

// Forwards a request arriving from the attached binding to the Iris network, and
// waits for a reply to arrive back which can be forwarded.
func (r *relay) handleRequest(cluster string, id uint64, request []byte, timeout time.Duration) {
	reply, err := r.iris.Request(cluster, request, timeout)
	switch {
	case err == iris.ErrTimeout || err == iris.ErrTerminating:
		r.sendReply(id, nil, "")
	case err != nil:
		r.sendReply(id, nil, err.Error())
	default:
		r.sendReply(id, reply, "")
	}
}

// Forwards a reply arriving from the attached binding to the Iris network by
// looking up the pending request channel and if still live, injecting the result.
func (r *relay) handleReply(id uint64, reply []byte, fault string) {
	r.reqLock.RLock()
	defer r.reqLock.RUnlock()

	// Fetch the result channels
	repc, ok := r.reqReps[id]
	if !ok {
		return
	}
	errc, ok := r.reqErrs[id]
	if !ok {
		panic("reply channel available, error missing")
	}
	// Return either the reply or the fault
	if reply == nil && len(fault) == 0 {
		errc <- iris.ErrTimeout
	} else if reply == nil {
		errc <- errors.New(fault)
	} else {
		repc <- reply
	}
}

// Handler for a topic subscription. Forwards all published events to the
// attached binding.
type subscriptionHandler struct {
	relay *relay
	topic string
}

// Forwards an arriving topic event from the Iris network to the attached
// binding.
func (s *subscriptionHandler) HandleEvent(msg []byte) {
	if err := s.relay.sendPublish(s.topic, msg); err != nil {
		log.Printf("relay: publish forward error: %v.", err)
		s.relay.drop()
	}
}

// Forwards a topic subscription arriving from the attached binding to the Iris
// node and creates a new subscription handler to process the published events.
func (r *relay) handleSubscribe(topic string) {
	// Create the event forwarder
	handler := &subscriptionHandler{
		relay: r,
		topic: topic,
	}
	// Subscribe and drop connection in case of an error
	if err := r.iris.Subscribe(topic, handler); err != nil {
		log.Printf("relay: subscription error: %v.", err)
		r.drop()
	}
}

// Forwards a topic subscription removal arriving from the attached binding to
// the Iris node.
func (r *relay) handleUnsubscribe(topic string) {
	if err := r.iris.Unsubscribe(topic); err != nil {
		log.Printf("relay: unsubscription error: %v.", err)
		r.drop()
	}
}

// Forwards a publish event arriving from the attached binding to the Iris node.
func (r *relay) handlePublish(topic string, msg []byte) {
	if err := r.iris.Publish(topic, msg); err != nil {
		log.Printf("relay: publish error: %v.", err)
		r.drop()
	}
}

// Forwards a tunnel initiation from the Iris network to the attached binding.
func (r *relay) HandleTunnel(tun *iris.Tunnel) {
	// Allocate a temporary tunnel
	r.tunLock.Lock()
	buildId := r.tunIdx
	init := make(chan struct{}, 1)
	r.tunInit[buildId] = init
	r.tunPend[buildId] = tun
	r.tunIdx++
	r.tunLock.Unlock()

	// Make sure the temporary tunnel is removed
	defer func() {
		r.tunLock.Lock()
		delete(r.tunInit, buildId)
		delete(r.tunPend, buildId)
		r.tunLock.Unlock()
	}()
	// Send a tunneling request to the attached binding
	if err := r.sendTunnelInit(buildId, config.RelayTunnelChunkLimit); err != nil {
		log.Printf("relay: tunnel request notification failed: %v.", err)
		r.drop()
	}
	// Wait for the final id and save the tunnel
	select {
	case <-time.After(config.RelayTunnelTimeout):
		log.Printf("relay: tunnel request timed out.")
		r.drop()
	case <-init:
	}
}

// Forwards a tunnel construction request from the attached binding to the Iris
// node and relay the result back to the binding.
func (r *relay) handleTunnelInit(id uint64, cluster string, timeout time.Duration) {
	// Create the tunnel
	tun, err := r.iris.Tunnel(cluster, timeout)
	if err != nil {
		if err := r.sendTunnelResult(id, 0); err != nil {
			log.Printf("relay: tunnel timeout notification error: %v.", err)
			r.drop()
		}
		return
	}
	// Insert the tunnel into the tracked ones
	r.tunLock.Lock()
	tunnel := r.newTunnel(id, tun)
	r.tunLive[id] = tunnel
	r.tunLock.Unlock()

	// Notify the attached binding of the success
	if err := r.sendTunnelResult(id, config.RelayTunnelChunkLimit); err != nil {
		log.Printf("relay: tunnel success notification error: %v.", err)
		r.drop()
		return
	}
	// Grant the local data allowance
	if err := r.sendTunnelAllowance(id, config.RelayTunnelBuffer); err != nil {
		log.Printf("relay: tunnel allowance grant error: %v.", err)
		r.drop()
		return
	}
	// Start the data transfer
	go tunnel.sender()
	go tunnel.receiver()
}

// Forwards a tunnel confirmation from the attached binding to the remote Iris
// endpoint and starts the data flow.
func (r *relay) handleTunnelConfirm(buildId uint64, tunId uint64) {
	r.tunLock.Lock()

	// Create the new relay tunnel
	tun, ok := r.tunPend[buildId]
	if !ok {
		log.Printf("relay: non-existent tunnel confirmed: %v.", buildId)
		return
	}
	tunnel := r.newTunnel(tunId, tun)
	r.tunLive[tunId] = tunnel

	// Signal the tunnel request of the successful initialization
	if init, ok := r.tunInit[buildId]; ok {
		init <- struct{}{}
	}
	r.tunLock.Unlock()

	// Critical section over, finish the initialization on a new thread
	r.workers.Schedule(func() {
		// Grant the local data allowance
		if err := r.sendTunnelAllowance(tunId, config.RelayTunnelBuffer); err != nil {
			log.Printf("relay: tunnel allowance grant error: %v.", err)
			r.drop()
			return
		}
		// Start the data transfer
		go tunnel.sender()
		go tunnel.receiver()
	})
}

// Grants some additional space allowance for the sender.
func (r *relay) handleTunnelAllowance(id uint64, space int) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	if tun, ok := r.tunLive[id]; ok {
		tun.grantAllowance(space)
	}
}

// Forwards a tunnel data packet from the attached binding into the correct
// endpoint.
func (r *relay) handleTunnelSend(id uint64, size int, payload []byte) {
	r.tunLock.RLock()
	defer r.tunLock.RUnlock()

	if tun, ok := r.tunLive[id]; ok {
		if err := tun.sendChunk(size, payload); err != nil {
			log.Printf("relay: tunnel send failed: %v.", err)
			r.drop()
		}
	}
}

// Terminates the tunnel data transfer threads and notifies the remote endpoint.
func (r *relay) handleTunnelClose(id uint64, local bool, reason string) {
	// Remove the tunnel
	r.tunLock.Lock()
	tun, ok := r.tunLive[id]
	delete(r.tunLive, id)
	r.tunLock.Unlock()

	if ok {
		// In case of a local close, signal the remote endpoint
		if local {
			go tun.tun.Close()
		}
		// Terminate the tunnel transfers
		go tun.close()

		// Signal the application of termination
		if err := r.sendTunnelClose(id, reason); err != nil {
			log.Printf("relay: tunnel close notification failed: %v", err)
			r.drop()
		}
	}
}
