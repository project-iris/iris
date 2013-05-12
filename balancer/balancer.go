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

// Package balancer implements a capacity based load balancer where each entity
// periodically reports its actual processing capacity and the balancer issues
// requests based on those numbers.
package balancer

import (
	"math/big"
	"math/rand"
	"sort"
	"sync"
)

// The load balancer for a single topic.
type Balancer struct {
	members  entitySlice  // Entries to which to balace to
	capacity int          // Total message capacity of the topic
	lock     sync.RWMutex // Mutex to allow reentrant balancing
}

// Creates a new - empty - load balancer.
func New() *Balancer {
	return &Balancer{
		members: []*entity{},
	}
}

// Registers an entity to load balance to (no duplicate checks are done).
func (b *Balancer) Register(id *big.Int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.members = append(b.members, &entity{id: id, cap: 1})
	sort.Sort(b.members)
	b.capacity += 1
}

// Unregisters an entity from the possible balancing destinations.
func (b *Balancer) Unregister(id *big.Int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	idx := b.members.Search(id)
	if idx < len(b.members) && b.members[idx].id.Cmp(id) == 0 {
		// Update total system capacity
		b.capacity -= b.members[idx].cap

		// Swap with last element
		last := len(b.members) - 1
		b.members[idx] = b.members[last]
		b.members = b.members[:last]

		// Get back to sorted order
		sort.Sort(b.members)
	} else {
		panic("trying to remove non-registered entity")
	}
}

// Updates an entry's capacity to cap.
func (b *Balancer) Update(id *big.Int, cap int) {
	b.lock.Lock()
	defer b.lock.Unlock()

	// Zero capacity is not allowed
	if cap <= 0 {
		cap = 1
	}

	idx := b.members.Search(id)
	if idx < len(b.members) && b.members[idx].id.Cmp(id) == 0 {
		// Update total system capacity
		b.capacity -= b.members[idx].cap
		b.capacity += cap

		// Update local capacity
		b.members[idx].cap = cap
	} else {
		panic("trying to update non-registered entity")
	}
}

// Returns an id to which to send the next message to. The optional src (can be
// nil) is used to exclude an entity from balancing to (if it's the only one
// available then this guarantee will be forfeit).
func (b *Balancer) Balance(src *big.Int) *big.Int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// Make sure there is actually somebody to balance to
	if b.capacity == 0 {
		panic("no capacity to balance")
	}
	// Calculate the available capacity with src excluded
	available := b.capacity
	exclude := -1
	if src != nil {
		idx := b.members.Search(src)
		if idx < len(b.members) && b.members[idx].id.Cmp(src) == 0 {
			if available != b.members[idx].cap {
				available -= b.members[idx].cap
				exclude = idx
			}
		}
	}
	// Generate a uniform random capacity and send to the associated entity
	cap := rand.Intn(available)
	for i, m := range b.members {
		// Skip the excluded source entity
		if i == exclude {
			continue
		}
		cap -= m.cap
		if cap < 0 {
			return m.id
		}
	}
	// Just in case to prevent bugs
	panic("balanced out of bounds")
}
