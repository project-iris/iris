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

// This file contains the implementation of topic maintenance and related ops.

package carrier

import (
	"balancer"
	"github.com/karalabe/cookiejar/exts/sortext"
	"math/big"
	"math/rand"
	"sync"
)

// Special id to reffer to the local node.
var localId = big.NewInt(-1)

// The maintenance data related to a single topic.
type topic struct {
	parent *big.Int   // Parent node in the topic tree
	nodes  []*big.Int // Remote children in the topic tree
	apps   []*big.Int // Local children in the topic tree

	bal *balancer.Balancer

	lock sync.RWMutex
}

// Creates a new topic with no subscriptions.
func newTopic() *topic {
	return &topic{
		nodes: []*big.Int{},
		apps:  []*big.Int{},
		bal:   balancer.New(),
	}
}

// Subscribes an entity to the local topic.
func (t *topic) subscribe(id *big.Int, app bool) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Choose the subscription type
	var list []*big.Int
	if app {
		list = t.apps
	} else {
		list = t.nodes
	}
	// Insert into the list if new
	idx := sortext.SearchBigInts(list, id)
	if idx >= len(list) || id.Cmp(list[idx]) != 0 {
		list = append(list, id)
		sortext.BigInts(list)
	}
	// Save the updated list and insert into balancer
	if app {
		if len(t.apps) == 0 {
			t.bal.Register(localId)
		}
		t.apps = list
	} else {
		if len(t.nodes) < len(list) {
			t.bal.Register(id)
		}
		t.nodes = list
	}
}

// Unsubscribes an enitity from the local topic.
func (t *topic) unsubscribe(id *big.Int, app bool) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	// Choose the subscription type
	var list []*big.Int
	if app {
		list = t.apps
	} else {
		list = t.nodes
	}
	// Remove from the list if exists
	idx := sortext.SearchBigInts(list, id)
	if idx < len(list) && id.Cmp(list[idx]) == 0 {
		last := len(list) - 1
		list[idx] = list[last]
		list = list[:last]
	}
	// Save the updated list
	if app {
		if len(t.apps) == 1 && len(list) == 0 {
			t.bal.Unregister(localId)
		}
		t.apps = list
	} else {
		if len(t.nodes) > len(list) {
			t.bal.Unregister(id)
		}
		t.nodes = list
	}
	return len(t.apps) == 0 && len(t.nodes) == 0
}

// Returns a node OR app id to which the balancer deemed the next message should
// be sent.
func (t *topic) balance(src *big.Int) (nodeId, appId *big.Int) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Pick a balance target and forward of remote node
	id := t.bal.Balance(src)
	if id.Cmp(localId) != 0 {
		return id, nil
	}
	// Otherwise balance between local apps
	return nil, t.apps[rand.Intn(len(t.apps))]
}
