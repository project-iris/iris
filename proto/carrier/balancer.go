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

// This file contains the load balancer logic for message distribution within a
// topic.

package carrier

import (
	"fmt"
	"math/big"
	"math/rand"
	"sync"
)

// The load balancer for a single topic.
type balancer struct {
	nodes    []entity     // Entries to which to balace to
	capacity int          // Total message capacity of the topic
	lock     sync.RWMutex // Mutex to allow reentrant balancing
}

// Entity and related measurements.
type entity struct {
	id  *big.Int // Overlay node id of the entity
	cap int      // Message capacity as reported by entity
}

// Creates a new - empty - load balancer.
func newBalancer() *balancer {
	return &balancer{
		nodes: []entity{},
	}
}

// Registers an entity to load balance to (no duplicate checks are done).
func (b *balancer) Register(id *big.Int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.nodes = append(b.nodes, entity{id: id})
}

// Unregisters an entity from the possible balancing destinations.
func (b *balancer) Unregister(id *big.Int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for i := 0; i < len(b.nodes); i++ {
		if id.Cmp(b.nodes[i].id) == 0 {
			// Update total system capacity
			b.capacity -= b.nodes[i].cap

			// Discard the entity
			last := len(b.nodes) - 1
			b.nodes[i] = b.nodes[last]
			b.nodes = b.nodes[:last]
			return
		}
	}
	// Just in case to prevent bugs
	panic("trying to remove non-registered entity")
}

// Updates an entry's capacity to cap.
func (b *balancer) Update(id *big.Int, cap int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Zero capacity is not allowed
	if cap <= 0 {
		cap = 1
	}
	for i := 0; i < len(b.nodes); i++ {
		if id.Cmp(b.nodes[i].id) == 0 {
			// Update total system capacity
			b.capacity -= b.nodes[i].cap
			b.capacity += cap

			// Update local capacity
			b.nodes[i].cap = cap
			return
		}
	}
	// Just in case to prevent bugs
	panic("trying to update non-registered entity")
}

// Returns an id to which to send the next message to. The optional src (can be
// nil) is used to exclude an entity from balancing to (if it's the only one
// available then this guarantee will be forfeit).
func (b *balancer) Balance(src *big.Int) (*big.Int, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// Make sure there is actually somebody to balance to
	if b.capacity == 0 {
		return nil, fmt.Errorf("no capacity to balance")
	}
	// Calculate the available capacity without the excluded entity
	available := b.capacity
	exclude := -1
	if src != nil {
		for i, node := range b.nodes {
			if src.Cmp(node.id) == 0 {
				if available != node.cap {
					available -= node.cap
					exclude = i
				}
				break
			}
		}
	}
	// Generate a uniform random capacity and send to the associated entity
	cap := rand.Intn(available)
	for i, node := range b.nodes {
		// Skip the excluded source entity
		if i == exclude {
			continue
		}
		cap -= node.cap
		if cap < 0 {
			return node.id, nil
		}
	}
	// Just in case to prevent bugs
	panic("balanced out of bounds")
}
