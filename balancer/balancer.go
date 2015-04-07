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

// Package balancer implements a capacity based load balancer where each entity
// periodically reports its actual processing capacity and the balancer issues
// requests based on those numbers.
package balancer

import (
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"sync"
)

// The load balancer for a single topic.
type Balancer struct {
	members  entitySlice  // Entries to which to balance to
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
func (b *Balancer) Update(id *big.Int, cap int) error {
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
		return fmt.Errorf("non-registered entity: %v", id)
	}
	return nil
}

// Returns an id to which to send the next message to. The optional ex (can be
// nil) is used to exclude an entity from balancing to (if it's the only one
// available then this guarantee will be forfeit).
func (b *Balancer) Balance(ex *big.Int) (*big.Int, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// Make sure there is actually somebody to balance to
	if b.capacity == 0 {
		return nil, fmt.Errorf("no capacity to balance")
	}
	// Calculate the available capacity with ex excluded
	available := b.capacity
	exclude := -1
	if ex != nil {
		idx := b.members.Search(ex)
		if idx < len(b.members) && b.members[idx].id.Cmp(ex) == 0 {
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
			return m.id, nil
		}
	}
	// Just in case to prevent bugs
	panic("balanced out of bounds")
}

// Returns the total capacity that the balancer can handle, optionally with ex
// excluded from the count.
func (b *Balancer) Capacity(ex *big.Int) int {
	b.lock.RLock()
	defer b.lock.RUnlock()

	// If nothing's excluded, return total capacity
	if ex == nil {
		return b.capacity
	}
	// Otherwise find the excluded node and return reduced capacity
	idx := b.members.Search(ex)
	if idx < len(b.members) && b.members[idx].id.Cmp(ex) == 0 {
		return b.capacity - b.members[idx].cap
	} else {
		return b.capacity
	}
}
