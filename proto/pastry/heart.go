// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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

// Contains the heartbeat mechanism, a beater thread which periodically pings
// all connected nodes (also adding whether they are considered active).

package pastry

import (
	"log"
	"math/big"
	"sync"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/heart"
)

// Heartbeat manager and callback handler for the overlay.
type heartbeat struct {
	owner *Overlay
	heart *heart.Heart
	beats sync.WaitGroup
}

// Creates a new heartbeat mechanism.
func newHeart(o *Overlay) *heartbeat {
	// Initialize a new heartbeat mechanism
	h := &heartbeat{
		owner: o,
	}
	// Insert the internal beater and return
	h.heart = heart.New(config.PastryBeatPeriod, config.PastryKillCount, h)

	return h
}

// Starts the heartbeats.
func (h *heartbeat) start() {
	h.heart.Start()
}

// Terminates the heartbeat mechanism.
func (h *heartbeat) terminate() error {
	err := h.heart.Terminate()
	h.beats.Wait()
	return err
}

// Periodically sends a heartbeat to all existing connections, tagging them
// whether they are active (i.e. in the routing) table or not.
func (h *heartbeat) Beat() {
	h.owner.lock.RLock()
	defer h.owner.lock.RUnlock()

	for _, p := range h.owner.livePeers {
		h.beats.Add(1)
		go func(p *peer, active bool) {
			defer h.beats.Done()
			h.owner.sendBeat(p, !active)
		}(p, h.owner.active(p.nodeId))
	}
}

// Implements heat.Callback.Dead, handling the event of a remote peer missing
// all its beats. The peers is reported dead and dropped.
func (h *heartbeat) Dead(id *big.Int) {
	log.Printf("pastry: remote peer reported dead: %v.", id)

	h.owner.lock.RLock()
	dead, ok := h.owner.livePeers[id.String()]
	h.owner.lock.RUnlock()

	if ok {
		h.owner.drop(dead)
	}
}
