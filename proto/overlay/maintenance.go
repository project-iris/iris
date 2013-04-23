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
	var routes *table
	for {
		// Copy the existing routing table if required
		if routes == nil {
			o.lock.RLock()
			routes = new(table)
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
		}
		addrs := make(map[string][]string)

		// Block till an update arrives
		select {
		case <-o.quit:
			return
		case s := <-o.upSink:
			o.merge(routes, addrs, s)
		}
		// Cascade merges and new connections until nobody else wants in
		for cascade := true; cascade; {
			cascade = false

			// Merge all pending updates to minimize state exchanges
			for idle := false; !idle; {
				select {
				case <-o.quit:
					return
				case s := <-o.upSink:
					o.merge(routes, addrs, s)
					cascade = true
				default:
					idle = true
				}
			}
			// Check the new table for discovered peers and dial each (all interfaces for now)
			if peers := o.discover(routes); len(peers) != 0 {
				fmt.Println(o.nodeId, "new peers to connect to:", peers)
				for _, id := range peers {
					for _, a := range addrs[id.String()] {
						if addr, err := net.ResolveTCPAddr("tcp", a); err != nil {
							log.Println("failed to resolve address %v: %v.", a, err)
						} else {
							o.dial(addr)
						}
					}
				}
				// Wait till all outbound connections either complete or timeout
				o.pend.Wait()

				// Do another round of discovery to find broken links and revert those entries
				if downs := o.discover(routes); len(downs) != 0 {
					o.revoke(routes, downs)
				}
			}
		}
		// Swap and broadcast if anything new
		if o.changed(routes) {
			o.lock.Lock()
			fmt.Printf("New routing table for %v:\n", o.nodeId)
			fmt.Printf("Leafset: %v\n", routes.leaves)
			fmt.Printf("Routes: %v\n", routes.routes)
			fmt.Printf("Neighbors: %v\n", routes.nears)
			fmt.Println("")
			o.routes, routes = routes, nil
			o.time++
			o.stat = done
			for _, peer := range o.pool {
				go o.sendState(peer)
			}
			o.lock.Unlock()
		}
	}
}

// Merges the recieved state into the provided routing table according to the
// pastry specs (neighborhood unimplemented for the moment). Also each peer's
// network address is saved for later use.
func (o *overlay) merge(t *table, a map[string][]string, s *state) {
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
		row, col := prefix(o.nodeId, id)
		old := t.routes[row][col]
		switch {
		case old == nil:
			t.routes[row][col] = id
		case old.Cmp(id) != 0:
			// log.Printf("routing entry exists, discarding (should do proximity magic instead): %v", id)
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
	sortext.BigInts(ids)
	return ids[:sortext.Unique(sortext.BigIntSlice(ids))]
}

// Revokes the list of unreachable peers from routing table t.
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
			oldId, newId := o.routes.routes[r][c], t.routes[r][c]
			switch {
			case newId == nil && oldId != nil:
				return true
			case newId != nil && oldId == nil:
				return true
			case newId == nil && oldId == nil:
				break
			case newId.Cmp(oldId) != 0:
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
