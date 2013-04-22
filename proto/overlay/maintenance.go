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
package overlay

import (
	"config"
	"fmt"
	"github.com/karalabe/cookiejar/exts/mathext"
	"github.com/karalabe/cookiejar/exts/sortext"
	"log"
	"math/big"
)

// Listens for incoming state merge requests, assembles new routing tables based
// on them, ensures all connections are live in the new table and swaps out the
// old one. Repeat.
func (o *overlay) merger() {
	for {
		// Copy the existing routing table
		routes := new(table)
		routes.leaves = make([]*big.Int, len(o.routes.leaves))
		copy(routes.leaves, o.routes.leaves)
		routes.routes = make([][]*big.Int, len(o.routes.routes))
		for i := 0; i < len(routes.routes); i++ {
			routes.routes[i] = make([]*big.Int, len(o.routes.routes[i]))
			copy(routes.routes[i], o.routes.routes[i])
		}
		routes.nears = make([]*big.Int, len(o.routes.nears))
		copy(routes.nears, o.routes.nears)

		// Wait till an update arrives
		select {
		case <-o.quit:
			return
		case s := <-o.upSink:
			o.merge(routes, s)
		}
		// Merge all updates before broadcasting new state
		for idle := false; !idle; {
			select {
			case <-o.quit:
				return
			case s := <-o.upSink:
				o.merge(routes, s)
			default:
				idle = true
			}
		}
		// Swap and broadcast if anything new
		rems, adds := o.diff(routes)
		if len(rems) != 0 || len(adds) != 0 {
			fmt.Println("Broadcasting new table:", routes)
			o.lock.Lock()
			o.routes = routes
			o.time++
			for _, peer := range o.pool {
				o.sendState(peer)
			}
			o.lock.Unlock()
		} else {
			fmt.Println("Nothing important.")
		}
	}
}

// Merges the recieved state into src and generates the new routing table.
func (o *overlay) merge(t *table, s *state) {
	fmt.Println("Updating state")

	// Extract the ids from the state exchange
	ids := make([]*big.Int, 0, len(s.Addrs))
	for sid, _ := range s.Addrs {
		if id, ok := new(big.Int).SetString(sid, 10); ok == true {
			// Skip loopback ids
			if o.nodeId.Cmp(id) != 0 {
				ids = append(ids, id)
			}
		} else {
			log.Printf("invalid node id received: %v.", sid)
		}
	}
	// Generate the new leaf set
	t.leaves = append(t.leaves, ids...)
	sortext.BigInts(t.leaves)
	unique := sortext.Unique(sortext.BigIntSlice(t.leaves))
	origin := sortext.SearchBigInts(t.leaves[:unique], o.nodeId)
	t.leaves = t.leaves[mathext.MaxInt(0, origin-config.PastryLeaves/2):mathext.MinInt(unique, origin+config.PastryLeaves/2)]

	// Merge the recieved addresses into the routing table
	for _, id := range ids {
		idx := prefix(o.nodeId, id)
		col := uint(0)
		for b := 0; b < config.PastryBase; b++ {
			col |= id.Bit(idx*config.PastryBase+b) << uint(b)
		}
		old := t.routes[idx][col]
		switch {
		case old == nil:
			t.routes[idx][col] = id
		case old.Cmp(id) != 0:
			log.Printf("routing entry exists, discarding (should do proximity magic instead): %v", id)
		}
	}
}

func (o *overlay) diff(t *table) (rems, adds []*big.Int) {
	// Diff and execute updates if needed
	rems, adds = diff(o.routes.leaves, t.leaves)

	return
}

// Collects the removals and additions needed to turn src slice into dst.
func diff(src, dst []*big.Int) (rems, adds []*big.Int) {
	rems, adds = []*big.Int{}, []*big.Int{}
	is, id := 0, 0
	for is < len(src) && id < len(dst) {
		switch src[is].Cmp(dst[id]) {
		case -1:
			rems = append(rems, src[is])
			is++
		case 1:
			adds = append(adds, dst[id])
			id++
		default:
			is++
			id++
		}
	}
	rems = append(rems, src[is:]...)
	adds = append(adds, dst[id:]...)
	return
}
