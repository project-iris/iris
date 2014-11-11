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

// Package topic implements a carrier topic tree with functionality to unicast,
// broadcast and load balance between nodes of the tree.
package topic

import (
	"errors"
	"math"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/project-iris/iris/balancer"
	"github.com/project-iris/iris/ext/sortext"
	"github.com/project-iris/iris/system"
)

// Custom topic error messages
var ErrSubscribed = errors.New("already subscribed")
var ErrNotSubscribed = errors.New("not subscribed")

// The maintenance data related to a single topic.
type Topic struct {
	id      *big.Int            // Unique id of the topic
	owner   *big.Int            // Id of the local node
	parent  *big.Int            // Parent node in the topic tree
	nodes   []*big.Int          // Remote children in the topic tree (+local if subbed)
	members map[string]struct{} // Membership set to allow fast lookups

	load *balancer.Balancer // Balancer to load-distribute messages
	msgs int32              // Number of messages balanced to locals (atomic, take care)

	lock sync.RWMutex
}

// Creates a new topic with no subscriptions.
func New(id, owner *big.Int) *Topic {
	// log.Printf("%v topic created: %v", owner, id)
	return &Topic{
		id:      id,
		owner:   owner,
		nodes:   []*big.Int{},
		members: make(map[string]struct{}),
		load:    balancer.New(),
	}
}

// Returns the topic identifier.
func (t *Topic) Self() *big.Int {
	return t.id
}

// Returns the current topic parent node.
func (t *Topic) Parent() *big.Int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.parent
}

// Sets the topic parent to the one specified.
func (t *Topic) Reown(parent *big.Int) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// log.Printf("%v:%v: changing ownership from %v to %v.", t.owner, t.id, t.parent, parent)

	// If an old parent existed, clear out leftovers
	if t.parent != nil {
		t.load.Unregister(t.parent)
		delete(t.members, t.parent.String())
	}
	// Initialize and save the new parent if any
	if parent != nil {
		t.load.Register(parent)
		t.members[parent.String()] = struct{}{}
	}
	t.parent = parent
}

// Returns whether the current topic subtree is empty.
func (t *Topic) Empty() bool {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return len(t.nodes) == 0
}

// Subscribes a node to the topic and inserts it into the load balancer registry.
func (t *Topic) Subscribe(id *big.Int) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// log.Printf("%v:%v: node subscription: %v.", t.owner, t.id, id)

	// Ensure double subscription doesn't happen
	idx := sortext.SearchBigInts(t.nodes, id)
	if idx < len(t.nodes) && id.Cmp(t.nodes[idx]) == 0 {
		return ErrSubscribed
	}
	// New entity, insert into the list
	t.nodes = append(t.nodes, id)
	sortext.BigInts(t.nodes)
	t.members[id.String()] = struct{}{}

	// log.Printf("%v:%v: subbed, state: %v.", t.owner, t.id, t.nodes)

	// Start load balancing to it too
	t.load.Register(id)
	return nil
}

// Unregisters a node from the topic, removing it from the balancer's registry.
func (t *Topic) Unsubscribe(id *big.Int) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	// log.Printf("%v:%v: node unsubscription: %v.", t.owner, t.id, id)

	// Ensure double unsubscription doesn't happen
	idx := sortext.SearchBigInts(t.nodes, id)
	if idx == len(t.nodes) || id.Cmp(t.nodes[idx]) != 0 {
		return ErrNotSubscribed
	}
	// Remove the node from the children
	last := len(t.nodes) - 1
	t.nodes[idx] = t.nodes[last]
	t.nodes = t.nodes[:last]
	sortext.BigInts(t.nodes)
	delete(t.members, id.String())

	// log.Printf("%v:%v: remed, state: %v.", t.owner, t.id, t.nodes)

	// Remove the node from the load balancer
	t.load.Unregister(id)
	return nil
}

// Returns whether a node is a neighbor of the current one in the topic tree.
func (t *Topic) Neighbor(id *big.Int) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()

	_, ok := t.members[id.String()]
	return ok
}

// Returns the list of nodes that a broadcast message should be sent to. An
// optional ex node can be specified to exclude it from the list.
func (t *Topic) Broadcast(ex *big.Int) []*big.Int {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Gather all the nodes to broadcast to
	nodes := make([]*big.Int, len(t.nodes), len(t.nodes)+1)
	copy(nodes, t.nodes)
	if t.parent != nil {
		nodes = append(nodes, t.parent)
	}
	// If exclusion is needed, do it
	if ex != nil {
		// Sort the nodes and do a binary search on them
		sortext.BigInts(nodes)
		idx := sortext.SearchBigInts(nodes, ex)

		// Swap out with the last if found
		if idx < len(nodes) && ex.Cmp(nodes[idx]) == 0 {
			last := len(nodes) - 1
			nodes[idx] = nodes[last]
			nodes = nodes[:last]
		}
	}
	return nodes
}

// Returns a node id to which the balancer deemed the next message should be
// sent. An optional ex node can be specified to prevent balancing there (if
// others exist).
func (t *Topic) Balance(ex *big.Int) (*big.Int, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Pick a balance target
	id, err := t.load.Balance(ex)
	if err != nil {
		return nil, err
	}
	// If the target is the local node, increment the task counter
	if id.Cmp(t.owner) == 0 {
		atomic.AddInt32(&t.msgs, 1)
	}
	return id, nil
}

// Returns the list of nodes to report to, and the report for each.
func (t *Topic) GenerateReports() ([]*big.Int, []int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Copy child list (rem local if subbed) + parent if not topic root
	ids := make([]*big.Int, len(t.nodes), len(t.nodes)+1)
	copy(ids, t.nodes)

	idx := sortext.SearchBigInts(ids, t.owner)
	if idx < len(ids) && t.owner.Cmp(ids[idx]) == 0 {
		last := len(ids) - 1
		ids[idx] = ids[last]
		ids = ids[:last]
	}
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
func (t *Topic) ProcessReport(id *big.Int, cap int) error {
	return t.load.Update(id, cap)
}

// If local subscriptions are alive in the topic, updates the balancer according
// to the messages processed since the last beat.
func (t *Topic) Cycle() {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Notify the balancer of the local capacity
	idx := sortext.SearchBigInts(t.nodes, t.owner)
	if idx < len(t.nodes) && t.owner.Cmp(t.nodes[idx]) == 0 {
		// Sanity check not to send some weird value
		cap := math.Max(0, float64(atomic.LoadInt32(&t.msgs))/float64(system.CpuUsage()))
		cap = math.Min(math.MaxInt32, cap)

		t.load.Update(t.owner, int(cap))
	}
	// Reset counters for next beat
	atomic.StoreInt32(&t.msgs, 0)
}
