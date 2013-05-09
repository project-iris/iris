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
	"github.com/karalabe/cookiejar/exts/sortext"
	"math/big"
	"sync"
)

// The maintenance data related to a single topic.
type topic struct {
	nodes []*big.Int
	apps  []*big.Int

	lock sync.RWMutex
}

// Creates a new topic with no subscriptions.
func newTopic() *topic {
	return &topic{
		nodes: []*big.Int{},
		apps:  []*big.Int{},
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
	// Save the updated list
	if app {
		t.apps = list
	} else {
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
		t.apps = list
	} else {
		t.nodes = list
	}
	return len(t.apps) == 0 && len(t.nodes) == 0
}
