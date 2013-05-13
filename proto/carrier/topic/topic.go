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

// Package topic implements a carrier topic tree with functionality to unicast,
// broadcast and load balance between nodes of the tree.
package topic

import (
	"balancer"
	"github.com/karalabe/cookiejar/exts/sortext"
	"heart"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Special id to reffer to the local node.
var localId = big.NewInt(-1)

// The maintenance data related to a single topic.
type Topic struct {
	id     *big.Int   // Unique id of the topic
	parent *big.Int   // Parent node in the topic tree
	nodes  []*big.Int // Remote children in the topic tree
	apps   []*big.Int // Local children in the topic tree

	heart *heart.Heart       // Heart-beat mechanism to notice dead/leaver nodes
	load  *balancer.Balancer // Balancer to load-distribute messages
	msgs  int32              // Number of messages balanced to locals (atomic, take care)

	lock sync.RWMutex
}

// Creates a new topic with no subscriptions.
func New(id *big.Int, beat time.Duration, kill int) *Topic {
	top := &Topic{
		id:    id,
		nodes: []*big.Int{},
		apps:  []*big.Int{},
		load:  balancer.New(),
	}
	// Create the heartbeat mechanism and start it
	top.heart = heart.New(beat, kill, top)
	top.heart.Start()

	return top
}

// Returns the topic identifier.
func (t *Topic) Self() *big.Int {
	return t.id
}

// Subscribes an application to the topic and inserts the local node into the
// load balancer.
func (t *Topic) SubscribeApp(id *big.Int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	idx := sortext.SearchBigInts(t.apps, id)
	if idx >= len(t.apps) || id.Cmp(t.apps[idx]) != 0 {
		// New entity, insert into the list
		t.apps = append(t.apps, id)
		sortext.BigInts(t.apps)

		// Start load balancing locally too if first
		if len(t.apps) == 1 {
			t.load.Register(localId)
		}
	}
}

// Subscribes a remote node to the topic, inserts it into the load balancer's
// registry and starts monitoring it for activity.
func (t *Topic) SubscribeNode(id *big.Int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	idx := sortext.SearchBigInts(t.nodes, id)
	if idx >= len(t.nodes) || id.Cmp(t.nodes[idx]) != 0 {
		// New entity, insert into the list
		t.nodes = append(t.nodes, id)
		sortext.BigInts(t.nodes)

		// Start monitoring and load balancing to it too
		t.heart.Monitor(id)
		t.load.Register(id)
	}
}

// Unsubscribes a local application from the topic and possibly removes the
// local node from the balancer if no apps remained. Returns whether the topic
// became empty.
func (t *Topic) UnsubscribeApp(id *big.Int) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	idx := sortext.SearchBigInts(t.apps, id)
	if idx < len(t.apps) && id.Cmp(t.apps[idx]) == 0 {
		// Remove the node from the children
		last := len(t.apps) - 1
		t.apps[idx] = t.apps[last]
		t.apps = t.apps[:last]

		// Create ordered list once again
		sortext.BigInts(t.apps)

		// Remove local apps from balancer if none left
		if len(t.apps) == 0 {
			t.load.Unregister(localId)
		}
	}
	// Return whether the topic became empty
	return len(t.apps) == 0 && len(t.nodes) == 0
}

// Unregisters a remote node from the topic, removing it from the balancer's
// registry as well as the heart-beaters monitor list. Returns whether the topic
// became empty.
func (t *Topic) UnsubscribeNode(id *big.Int) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	idx := sortext.SearchBigInts(t.nodes, id)
	if idx < len(t.nodes) && id.Cmp(t.nodes[idx]) == 0 {
		// Remove the node from the load balancer and heart monitor
		t.load.Unregister(t.nodes[idx])
		t.heart.Unmonitor(t.nodes[idx])

		// Remove the node from the children
		last := len(t.nodes) - 1
		t.nodes[idx] = t.nodes[last]
		t.nodes = t.nodes[:last]

		// Create ordered list once again
		sortext.BigInts(t.nodes)
	}
	// Return whether the topic became empty
	return len(t.apps) == 0 && len(t.nodes) == 0
}

// Returns the list of nodes and apps that a broadcast message should be sent.
func (t *Topic) Broadcast() ([]*big.Int, []*big.Int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	nodes := make([]*big.Int, len(t.nodes))
	copy(nodes, t.nodes)

	apps := make([]*big.Int, len(t.apps))
	copy(apps, t.apps)

	return nodes, apps
}

// Returns a node or an application id to which the balancer deemed the next
// message should be sent. An optional ex node can be specified to prevent
// balancing there (if others exist). Only one result will be valid, the other
// nil.
func (t *Topic) Balance(ex *big.Int) (nodeId, appId *big.Int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Pick a balance target and forward if remote node
	id := t.load.Balance(ex)
	if id.Cmp(localId) != 0 {
		return id, nil
	}
	// Otherwise balance between local apps
	atomic.AddInt32(&t.msgs, 1)
	return nil, t.apps[rand.Intn(len(t.apps))]
}

// Returns the list of nodes to report to, and the report for each.
func (t *Topic) GenerateReport() ([]*big.Int, []int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Copy child list + parent if not topic root
	ids := make([]*big.Int, len(t.nodes), len(t.nodes)+1)
	copy(ids, t.nodes)
	if t.parent != nil {
		ids = append(ids, t.parent)
	}
	// Calculate the capacities that should be reported to each
	caps := make([]int, len(ids))
	for i, id := range ids {
		caps[i] = t.load.Capacity(id)
	}
	// Return the capacity with the nodes to report to
	return ids, caps
}

// Sets the load capacity for a source node in the balancer.
func (t *Topic) ProcessReport(src *big.Int, cap int) {
	// Update the capacity in the load balancer
	t.load.Update(src, cap)

	// Notify the heartbeater of the source presence
	t.heart.Ping(src)
}
