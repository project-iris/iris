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

// This file contains the heartbeat event handlers and the related maintenance
// logic.

package topic

import (
	"github.com/karalabe/iris/system"
	"math"
	"math/big"
	"sync/atomic"
)

// Implements the heart.Callback.Beat method. If local subscriptions are alive
// in the topic, updates the balancer according to the messages processed since
// the last beat.
func (t *Topic) Beat() {
	t.lock.RLock()
	defer t.lock.RUnlock()

	// Notify the balancer of the local capacity
	if len(t.apps) > 0 {
		// Sanity check not to send some weird value
		cap := math.Max(0, float64(atomic.LoadInt32(&t.msgs))/float64(system.CpuUsage()))
		cap = math.Min(math.MaxInt32, cap)

		t.load.Update(localId, int(cap))
	}
	// Reset counters for next beat
	atomic.StoreInt32(&t.msgs, 0)
}

// Implements the heart.Callback.Dead method. Either a child or the parent node
// was reported dead, remove them.
func (t *Topic) Dead(id *big.Int) {
	// Handle the dead parent
	t.lock.Lock()
	parent := false
	if t.parent != nil && t.parent.Cmp(id) == 0 {
		t.parent = nil
		parent = true
	}
	t.lock.Unlock()

	// Handle dead children
	if !parent {
		t.UnsubscribeNode(id)
	}
}
