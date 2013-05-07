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

// Contains the overlay and routing table management functionality: one manager
// go-routine which processes state updates from all connected peers, merging
// them into the local state and connecting nodes discovered. It is also the one
// responsible for dropping failed and passive connections, whilse ensuring a
// valid routing table.
//
// The overlay heartbeat mechanism is also implemented here: a beater thread
// which periodically pings all connected nodes (also adding whether they are
// considered active).

package overlay

import (
	"config"
	"github.com/karalabe/cookiejar/exts/mathext"
	"github.com/karalabe/cookiejar/exts/sortext"
	"log"
	"math/big"
	"net"
	"sort"
	"sync"
	"time"
)

// Listens for incoming state merge requests, assembles new routing tables based
// on them, ensures all connections are live in the new table and swaps out the
// old one. Repeat. Also removes connections that either failed or were deemed
// useless.
func (o *Overlay) manager() {
	var pending sync.WaitGroup
	var routes *table
	for {
		// Copy the existing routing table if required
		if routes == nil {
			o.lock.RLock()
			routes = o.routes.Copy()
			o.lock.RUnlock()
		}
		addrs := make(map[string][]string)

		// Block till an update or drop arrives
		select {
		case <-o.quit:
			return
		case s := <-o.upSink:
			o.merge(routes, addrs, s)
		case d := <-o.dropSink:
			o.drop(d)
		}
		// Cascade merges, drops and new connections until nobody else wants in or out
		for cascade := true; cascade; {
			cascade = false
			for idle := false; !idle; {
				select {
				case <-o.quit:
					return
				case s := <-o.upSink:
					o.merge(routes, addrs, s)
					cascade = true
				case d := <-o.dropSink:
					o.drop(d)
					cascade = true
				default:
					idle = true
				}
			}
			// Check the new table for discovered peers and dial each (all interfaces for now)
			if peers := o.discover(routes); len(peers) != 0 {
				for _, id := range peers {
					for _, a := range addrs[id.String()] {
						if addr, err := net.ResolveTCPAddr("tcp", a); err != nil {
							log.Printf("failed to resolve address %v: %v.", a, err)
						} else {
							pending.Add(1)
							go func() {
								defer pending.Done()
								o.dial(addr)
							}()
						}
					}
				}
				// Wait till all outbound connections either complete or timeout
				pending.Wait()

				// Do another round of discovery to find broken links and revert/remove those entries
				if downs := o.discover(routes); len(downs) != 0 {
					o.revoke(routes, downs)
				}
			}
		}
		// Swap and broadcast if anything changed
		if ch, rep := o.changed(routes); ch {
			o.lock.Lock()
			o.routes, routes = routes, nil
			o.time++
			o.stat = done
			o.lock.Unlock()

			// Revert to read lock and broadcast state
			o.lock.RLock()
			for _, peer := range o.pool {
				go o.sendState(peer, rep)
			}
			o.lock.RUnlock()
		}
	}
}

// Drops an active peer connection due to either a failure or uselessness.
func (o *Overlay) drop(d *peer) {
	o.lock.Lock()
	defer o.lock.Unlock()

	// Make sure we kill it only once
	if !d.killed {
		d.killed = true
		close(d.quit)
	}
	// Clear up leftovers
	for id, p := range o.pool {
		if d == p {
			delete(o.pool, id)
			for _, addr := range d.addrs {
				delete(o.trans, addr)
			}
			break
		}
	}
}

// Merges the recieved state into the provided routing table according to the
// pastry specs (neighborhood unimplemented for the moment). Also each peer's
// network address is saved for later use.
func (o *Overlay) merge(t *table, a map[string][]string, s *state) {
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
	t.leaves = o.mergeLeaves(t.leaves, ids)

	// Merge the received addresses into the routing table
	for _, id := range ids {
		row, col := prefix(o.nodeId, id)
		old := t.routes[row][col]
		switch {
		case old == nil:
			t.routes[row][col] = id
		case old.Cmp(id) != 0:
			// TODO: Proximity magic
		}
	}
	// Merge the neighborhood set (TODO)
}

// Merges two leafsets and returns the result.
func (o *Overlay) mergeLeaves(a, b []*big.Int) []*big.Int {
	// Append, circular sort and fetch uniques
	res := append(a, b...)
	sort.Sort(idSlice{o.nodeId, res})
	res = res[:sortext.Unique(idSlice{o.nodeId, res})]

	// Look for the origin point
	origin := 0
	for o.nodeId.Cmp(res[origin]) != 0 {
		origin++
	}
	// Fetch the nearest nodes in both directions
	min := mathext.MaxInt(0, origin-config.OverlayLeaves/2)
	max := mathext.MinInt(len(res), origin+config.OverlayLeaves/2)
	return res[min:max]
}

// Searches a potential routing table for nodes not yet connected.
func (o *Overlay) discover(t *table) []*big.Int {
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
func (o *Overlay) revoke(t *table, downs []*big.Int) {
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
		// Repair the leafset as best as possible from the pool of active connections
		o.lock.RLock()
		all := make([]*big.Int, 0, len(o.pool))
		for _, p := range o.pool {
			all = append(all, p.nodeId)
		}
		o.lock.RUnlock()
		t.leaves = o.mergeLeaves(t.leaves, all)
	}
	// Clean up the routing table
	for r, row := range t.routes {
		for i, id := range row {
			if id != nil {
				idx := sortext.SearchBigInts(downs, id)
				if idx < len(downs) && downs[idx].Cmp(id) == 0 {
					// Try and fix routing entry from connection pool
					t.routes[r][i] = nil
					o.lock.RLock()
					for _, p := range o.pool {
						if pre, dig := prefix(o.nodeId, p.nodeId); pre == r && dig == i {
							t.routes[r][i] = p.nodeId
							break
						}
					}
					o.lock.RUnlock()
				}
			}
		}
	}
	// Clean up the neighborhood set (TODO)
}

// Checks whether the routing table changed and if yes, whether it needs repairs.
func (o *Overlay) changed(t *table) (ch bool, rep bool) {
	// Check the leaf set
	if len(t.leaves) != len(o.routes.leaves) {
		ch = true
	} else {
		for i := 0; i < len(t.leaves); i++ {
			if t.leaves[i].Cmp(o.routes.leaves[i]) != 0 {
				ch = true
				break
			}
		}
	}
	// Check the routing table
	for r := 0; r < len(t.routes); r++ {
		for c := 0; c < len(t.routes[0]); c++ {
			oldId, newId := o.routes.routes[r][c], t.routes[r][c]
			switch {
			case newId == nil && oldId != nil:
				ch, rep = true, true
			case newId != nil && oldId == nil:
				ch = true
			case newId == nil && oldId == nil:
				// Do nothing
			case newId.Cmp(oldId) != 0:
				ch = true
			}
		}
	}
	// Check the neighbor set
	if len(t.nears) != len(o.routes.nears) {
		ch = true
	} else {
		for i := 0; i < len(t.nears); i++ {
			if t.nears[i].Cmp(o.routes.nears[i]) != 0 {
				ch = true
				break
			}
		}
	}
	return
}

// Periodically sends a heatbeat to all existing connections, tagging them
// whether they are active (i.e. in the routing) table or not.
func (o *Overlay) beater() {
	tick := time.Tick(time.Duration(config.OverlayBeatPeriod) * time.Millisecond)
	for {
		select {
		case <-o.quit:
			return
		case <-tick:
			o.lock.RLock()
			for _, p := range o.pool {
				go o.sendBeat(p, !o.active(p.nodeId))
			}
			o.lock.RUnlock()
		}
	}
}

// Returns whether a connection is active or passive.
func (o *Overlay) active(p *big.Int) bool {
	for _, id := range o.routes.leaves {
		if p.Cmp(id) == 0 {
			return true
		}
	}
	for _, row := range o.routes.routes {
		for _, id := range row {
			if id != nil {
				if p.Cmp(id) == 0 {
					return true
				}
			}
		}
	}
	for _, id := range o.routes.nears {
		if p.Cmp(id) == 0 {
			return true
		}
	}
	return false
}
