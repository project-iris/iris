// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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
