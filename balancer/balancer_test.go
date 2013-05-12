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

package balancer

import (
	"math/big"
	"math/rand"
	"testing"
)

func TestBalancer(t *testing.T) {
	entities := 10
	threads := 100

	// Generate a handful of nodes
	ids := make([]*big.Int, entities)
	for i := 0; i < len(ids); i++ {
		ids[i] = big.NewInt(rand.Int63())
	}
	// Assign a capacity to each of them
	caps := make([]int, entities)
	for i := 0; i < len(caps); i++ {
		caps[i] = rand.Intn(1000) + 1
	}
	// Create the balancer and register all entities
	bal := New()
	for i := 0; i < entities; i++ {
		bal.Register(ids[i])
		bal.Update(ids[i], caps[i])
	}
	// Balance N x total capacity on separate threads each
	total := 0
	for _, cap := range caps {
		total += cap
	}
	res := make(chan *big.Int, total)
	for i := 0; i < threads; i++ {
		go func(idx int) {
			for c := 0; c < total; c++ {
				res <- bal.Balance(nil)
			}
		}(i)
	}
	// Also balance entities x total capacity - cap[i] to check excluding
	for i := 0; i < entities; i++ {
		go func(idx int) {
			for c := 0; c < total-caps[idx]; c++ {
				res <- bal.Balance(ids[idx])
			}
		}(i)
	}
	// Collect the balance results and verify histogram
	hist := make(map[string]int)
	for i := 0; i < entities; i++ {
		hist[ids[i].String()] = 0
	}
	for i := 0; i < (threads+entities-1)*total; i++ {
		id := <-res
		old := hist[id.String()]
		hist[id.String()] = old + 1
	}
	for i := 0; i < entities; i++ {
		// Calculate the balanced vs. capacity diff
		diff := caps[i] - hist[ids[i].String()]/(threads+entities-1)
		if diff < 0 {
			diff *= -1
		}
		// Report anything above 3% error (high enough to pass, low enough to catch anomalies)
		if float64(diff)/float64(caps[i]) > 0.03 {
			t.Errorf("unbalanced frequency: diff %v, cap %v.", diff, caps[i])
		}
	}
}
