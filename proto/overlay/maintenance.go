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
	"net"
)

// Listens for incoming state merge requests, assembles new routing tables based
// on them, ensures all connections are live in the new table and swaps out the
// old one. Repeat.
func (o *overlay) merger() {
	for {
		// Copy the existing routing table
		o.lock.RLock()
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
		o.lock.RUnlock()

		addrs := make(map[string][]string)

		// Wait till an update arrives
		select {
		case <-o.quit:
			return
		case s := <-o.upSink:
			o.merge(routes, addrs, s)
		}
		// Merge all pending updates
		for idle := false; !idle; {
			select {
			case <-o.quit:
				return
			case s := <-o.upSink:
				o.merge(routes, addrs, s)
			default:
				idle = true
			}
		}
		// Check the new table for discovered peers and dial each
		if peers := o.discover(routes); len(peers) != 0 {
			for _, id := range peers {
				// HACK: addrs[0]
				if addr, err := net.ResolveTCPAddr("tcp", addrs[id.String()][0]); err != nil {
					log.Println("failed to resolve address %v: %v.", addrs[id.String()][0], err)
				} else {
					o.dial(addr)
				}
			}
			o.pend.Wait()
		}
		// Do another round of discovery to find broken links and revert those entries
		if downs := o.discover(routes); len(downs) != 0 {
			o.revoke(routes, downs)
		}

		// Swap and broadcast if anything new
		if o.changed(routes) {
			fmt.Println(o.nodeId, "bcast", routes)
			o.lock.Lock()
			o.routes = routes
			o.time++
			for _, peer := range o.pool {
				o.sendState(peer)
			}
			o.lock.Unlock()
		} else {
			fmt.Println(o.nodeId, "no useful state")
		}
	}
}

// Merges the recieved state into src and generates the new routing table.
func (o *overlay) merge(t *table, a map[string][]string, s *state) {
	fmt.Println(o.nodeId, "merging")

	// Extract the ids from the state exchange
	ids := make([]*big.Int, 0, len(s.Addrs))
	for sid, addrs := range s.Addrs {
		if id, ok := new(big.Int).SetString(sid, 10); ok == true {
			// Skip loopback ids
			if o.nodeId.Cmp(id) != 0 {
				ids = append(ids, id)
				a[sid] = addrs
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

	// Merge the received addresses into the routing table
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
	// Merge the neighborhood set (TODO)
}

// Searches a potential routing table for nodes not yet connected.
func (o *overlay) discover(t *table) []*big.Int {
	o.lock.RLock()
	defer o.lock.RUnlock()

	ids := []*big.Int{}
	for _, id := range t.leaves {
		if id.Cmp(o.nodeId) != 0 {
			if _, ok := o.pool[id.String()]; !ok {
				ids = append(ids, id)
			}
		}
	}
	for _, row := range t.routes {
		for _, id := range row {
			if id != nil {
				if _, ok := o.pool[id.String()]; !ok {
					ids = append(ids, id)
				}
			}
		}
	}
	for _, id := range t.nears {
		if _, ok := o.pool[id.String()]; !ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// Revokes a list of peers from t which could not be reached.
func (o *overlay) revoke(t *table, downs []*big.Int) {
	sortext.BigInts(downs)

	// Clean up the leaf set
	intact := true
	for i := 0; i < len(t.leaves); i++ {
		idx := sortext.SearchBigInts(downs, t.leaves[i])
		if idx < len(downs) && downs[idx].Cmp(t.leaves[i]) == 0 {
			t.leaves[i] = t.leaves[len(t.leaves)-1]
			t.leaves = t.leaves[:len(t.leaves)-1]
			intact = false
			i--
		}
	}
	if !intact {
		t.leaves = append(t.leaves, o.routes.leaves...)
		sortext.BigInts(t.leaves)
		unique := sortext.Unique(sortext.BigIntSlice(t.leaves))
		origin := sortext.SearchBigInts(t.leaves[:unique], o.nodeId)
		t.leaves = t.leaves[mathext.MaxInt(0, origin-config.PastryLeaves/2):mathext.MinInt(unique, origin+config.PastryLeaves/2)]
	}
	// Clean up the routing table
	for r, row := range o.routes.routes {
		for i, id := range row {
			idx := sortext.SearchBigInts(downs, id)
			if idx < len(downs) && downs[idx].Cmp(id) == 0 {
				t.routes[r][i] = row[i]
			}
		}
	}
	// Clean up the neighborhood set (TODO)
}

// Checks whether the routing table changed.
func (o *overlay) changed(t *table) bool {
	// Check the leaf set
	if len(t.leaves) != len(o.routes.leaves) {
		return true
	}
	for i := 0; i < len(t.leaves); i++ {
		if t.leaves[i].Cmp(o.routes.leaves[i]) != 0 {
			return true
		}
	}
	// Check the routing table
	for r := 0; r < len(t.routes); r++ {
		for c := 0; c < len(t.routes[0]); c++ {
			switch {
			case t.routes[r][c] == nil && o.routes.routes[r][c] != nil:
				return true
			case t.routes[r][c] != nil && o.routes.routes[r][c] == nil:
				return true
			case t.routes[r][c] == nil && o.routes.routes[r][c] == nil:
				break
			case t.routes[r][c].Cmp(o.routes.routes[r][c]) != 0:
				return true
			}
		}
	}
	// Check the neighbor set
	if len(t.nears) != len(o.routes.nears) {
		return true
	}
	for i := 0; i < len(t.nears); i++ {
		if t.nears[i].Cmp(o.routes.nears[i]) != 0 {
			return true
		}
	}
	return false
}
