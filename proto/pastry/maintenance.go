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

// Contains the pastry and routing table management functionality: one manager
// go-routine which processes state updates from all connected peers, merging
// them into the local state and connecting discovered nodes. It is also the one
// responsible for dropping failed and passive connections, while ensuring a
// valid routing table.

package pastry

import (
	"log"
	"math/big"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/project-iris/iris/pool"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/ext/mathext"
	"github.com/project-iris/iris/ext/sortext"
)

// Listens for incoming state merge requests, assembles new routing tables based
// on them, ensures all connections are live in the new table and swaps out the
// old one. Repeat. Also removes connections that either failed or were deemed
// useless.
func (o *Overlay) manager() {
	var pending sync.WaitGroup
	var routes *table

	addrs := make(map[string][]string)
	exchs := make(map[*peer]*state)
	drops := make(map[*peer]struct{})

	// Mark the overlay as unstable
	stable := false
	stableTime := config.PastryBootTimeout

	var errc chan error
	for errc == nil {
		// Copy the existing routing table if required
		if routes == nil {
			o.lock.RLock()
			routes = o.routes.copy()
			o.lock.RUnlock()
		}
		// Reset any used temporary notification sets
		if len(addrs) > 0 {
			addrs = make(map[string][]string)
		}
		if len(exchs) > 0 {
			exchs = make(map[*peer]*state)
		}
		if len(drops) > 0 {
			drops = make(map[*peer]struct{})
		}
		// Block till an event arrives
		select {
		case errc = <-o.maintQuit:
			// Termination requested
			continue
		case <-o.eventNotify:
			// Swap out the collectors
			o.eventLock.Lock()
			o.exchSet, exchs = exchs, o.exchSet
			o.dropSet, drops = drops, o.dropSet
			o.eventLock.Unlock()

			// If stale notification, loop
			if len(exchs) == 0 && len(drops) == 0 {
				continue
			}
		case <-time.After(stableTime):
			// No update arrived for a while, consider stable
			if !stable {
				stable = true
				o.stable.Done()
			}
			continue
		}
		// Mark overlay as unstable and set a reduced convergence time
		if stable {
			stable = false
			o.stable.Add(1)
		}
		stableTime = config.PastryConvTimeout

		// Merge all state exchanges into the temporary routing table and drop unneeded nodes
		for _, s := range exchs {
			o.merge(routes, addrs, s)
		}
		o.dropAll(drops, &pending)

		// Check the new table for discovered peers and dial each
		if peers := o.discover(routes); len(peers) > 0 {
			for _, id := range peers {
				// Collect all the network interfaces
				peerAddrs := make([]*net.TCPAddr, 0, len(addrs[id.String()]))
				for _, address := range addrs[id.String()] {
					if addr, err := net.ResolveTCPAddr("tcp", address); err != nil {
						log.Printf("pastry: failed to resolve address %v: %v.", address, err)
					} else {
						peerAddrs = append(peerAddrs, addr)
					}
				}
				// Initiate a connection to the remote peer (make sure the lock is not lost)
				pending.Add(1)
				err := o.authInit.Schedule(func() {
					defer pending.Done()
					o.dial(peerAddrs)
				})
				if err == pool.ErrTerminating {
					pending.Done()
				}
			}
			// Wait till all outbound connections either complete or timeout
			pending.Wait()

			// Do another round of discovery to find broken links and revert/remove those entries
			if downs := o.discover(routes); len(downs) > 0 {
				o.revoke(routes, downs)
			}
		}
		// Swap and broadcast if anything changed
		if ch, rep := o.changed(routes); ch {
			o.lock.Lock()
			o.routes, routes = routes, nil
			o.time++
			o.stat = done
			o.lock.Unlock()

			// Revert to read lock (don't hold up reads) and broadcast state
			o.lock.RLock()
			o.stateExch.Clear()
			for _, p := range o.livePeers {
				p := p // Copy for closure!
				if rep {
					o.stateExch.Schedule(func() { o.sendRepair(p) })
				} else {
					o.stateExch.Schedule(func() { o.sendState(p) })
				}
			}
			o.lock.RUnlock()
		}
	}
	// Manager is terminating, drop all peer connections (not synced, no mods allowed)
	for _, p := range o.livePeers {
		pending.Add(1)
		go func(p *peer) {
			defer pending.Done()
			// Send a pastry leave to the remote node and wait
			o.sendClose(p)

			// Wait a while for remote tear-down
			select {
			case <-p.drop:
			case <-time.After(time.Second):
				log.Printf("pastry: graceful session close timed out.")
			}
			// Success or not, close the session
			if err := p.Close(); err != nil {
				log.Printf("pastry: failed to close peer during termination: %v.", err)
			}
		}(p)
	}
	pending.Wait()

	errc <- nil
}

// Inserts a state exchange into the exchange queue
func (o *Overlay) exch(p *peer, s *state) {
	// Insert the state exchange
	o.eventLock.Lock()
	o.exchSet[p] = s
	o.eventLock.Unlock()

	// Wake the manager if blocking
	select {
	case o.eventNotify <- struct{}{}:
		// Notification sent
	default:
		// Notification already pending
	}
}

// Inserts a peer into the drop queue.
func (o *Overlay) drop(p *peer) {
	// Insert the drop request
	o.eventLock.Lock()
	o.dropSet[p] = struct{}{}
	o.eventLock.Unlock()

	// Wake the manager if blocking
	select {
	case o.eventNotify <- struct{}{}:
		// Notification sent
	default:
		// Notification already pending
	}
}

// Drops an active peer connection due to either a failure or uselessness.
func (o *Overlay) dropAll(peers map[*peer]struct{}, pending *sync.WaitGroup) {
	// Make sure there's actually something to remove
	if len(peers) == 0 {
		return
	}
	// Close the peer connections (new thread, since close might block a while)
	for d, _ := range peers {
		pending.Add(1)
		go func(p *peer) {
			defer pending.Done()
			if err := p.Close(); err != nil {
				log.Printf("pastry: failed to close peer connection: %v.", err)
			}
		}(d)
	}
	// Ensure the overlay state needs change before acquiring expensive write lock
	change := false
	o.lock.RLock()
	for d, _ := range peers {
		if p, ok := o.livePeers[d.nodeId.String()]; ok && p == d {
			change = true
			break
		}
	}
	o.lock.RUnlock()
	if !change {
		return
	}
	// Remove the peers from the overlay state
	o.lock.Lock()
	defer o.lock.Unlock()

	for d, _ := range peers {
		id := d.nodeId.String()
		if p, ok := o.livePeers[id]; ok && p == d {
			// Delete the peer and stop monitoring it
			delete(o.livePeers, id)
			o.heart.heart.Unmonitor(d.nodeId)
		}
	}
}

// Merges the received state into the provided routing table according to the
// reduced pastry specs (no neighborhood sets). Also each peers network addresses
// are collected to connect later if needed.
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
			log.Printf("pastry: invalid node id received: %v.", sid)
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
			// Discard new entry (less disruptive)
		}
	}
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
	min := mathext.MaxInt(0, origin-config.PastryLeaves/2)
	max := mathext.MinInt(len(res), origin+config.PastryLeaves/2)
	return res[min:max]
}

// Searches a potential routing table for nodes not yet connected.
func (o *Overlay) discover(t *table) []*big.Int {
	o.lock.RLock()
	defer o.lock.RUnlock()

	ids := []*big.Int{}
	for _, id := range t.leaves {
		if id.Cmp(o.nodeId) != 0 {
			if _, ok := o.livePeers[id.String()]; !ok {
				ids = append(ids, id)
			}
		}
	}
	for _, row := range t.routes {
		for _, id := range row {
			if id != nil {
				if _, ok := o.livePeers[id.String()]; !ok {
					ids = append(ids, id)
				}
			}
		}
	}
	sortext.BigInts(ids)
	return ids[:sortext.Unique(sortext.BigIntSlice(ids))]
}

// Revokes the list of unreachable peers from routing table t.
func (o *Overlay) revoke(t *table, down []*big.Int) {
	downs := sortext.BigIntSlice(down)
	downs.Sort()

	// Clean up the leaf set
	intact := true
	for i := 0; i < len(t.leaves); i++ {
		idx := downs.Search(t.leaves[i])
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
		all := make([]*big.Int, 0, len(o.livePeers))
		for _, p := range o.livePeers {
			all = append(all, p.nodeId)
		}
		o.lock.RUnlock()
		t.leaves = o.mergeLeaves(t.leaves, all)
	}
	// Clean up the routing table
	for r, row := range t.routes {
		for c, id := range row {
			if id != nil {
				if idx := downs.Search(id); idx < len(downs) && downs[idx].Cmp(id) == 0 {
					// Try and fix routing entry from connection pool
					t.routes[r][c] = nil
					o.lock.RLock()
					for _, p := range o.livePeers {
						if pre, dig := prefix(o.nodeId, p.nodeId); pre == r && dig == c {
							t.routes[r][c] = p.nodeId
							break
						}
					}
					o.lock.RUnlock()
				}
			}
		}
	}
}

// Checks whether the routing table changed and if yes, whether it needs repairs.
func (o *Overlay) changed(t *table) (bool, bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	var change bool

	// Check the leaf set
	if len(t.leaves) < len(o.routes.leaves) {
		// We lost a live leaf, try to repair
		return true, true
	}
	if len(t.leaves) != len(o.routes.leaves) {
		change = true
	} else {
		for i := 0; i < len(t.leaves) && !change; i++ {
			if t.leaves[i].Cmp(o.routes.leaves[i]) != 0 {
				change = true
			}
		}
	}
	// Check the routing table
	for r := 0; r < len(t.routes); r++ {
		for c := 0; c < len(t.routes[0]); c++ {
			oldId, newId := o.routes.routes[r][c], t.routes[r][c]
			switch {
			case newId == nil && oldId != nil:
				// We lost a needed peer, request repairs
				return true, true
			case newId != nil && oldId == nil:
				// We gained a new peer, only signal change
				change = true
			case newId == nil && oldId == nil:
				// Do nothing
			case newId.Cmp(oldId) != 0:
				// An old peer was replaced, signal change
				change = true
			}
		}
	}
	return change, false
}

// Returns whether a connection is active or passive.
// Take care, this is called while locked (don't double lock).
func (o *Overlay) active(id *big.Int) bool {
	// Check whether id is an active leaf
	for _, leaf := range o.routes.leaves {
		if id.Cmp(leaf) == 0 {
			return true
		}
	}
	// Check whether id is an active table cell
	for _, row := range o.routes.routes {
		for _, cell := range row {
			if cell != nil && id.Cmp(cell) == 0 {
				return true
			}
		}
	}
	return false
}
