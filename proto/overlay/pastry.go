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
	"math/big"
	"proto/session"
)

// Id type in the virtual network.
type id *big.Int

// Routing state table for the pastry network.
type table struct {
	leaves []id
	routes [][]id
	nears  []id
}

// Creates a new empty pastry state table.
func newTable() (t *table) {
	t = new(table)
	t.leaves = make([]id, config.PastryLeaves)
	t.routes = make([][]id, config.PastrySpace/config.PastryBase)
	for i := 0; i < len(t.routes); i++ {
		t.routes[i] = make([]id, 1<<config.PastryBase)
	}
	t.nears = make([]id, config.PastryNeighbors)
	return
}

// Manages the pastry state table and connection pool by accepting new incomming
// sessions and boostrap events, deciding which ones to keep and which to drop.
func (o *overlay) manager() {
	for {
		select {
		case <-o.quit:
			return
		case sess := <-o.sessSink:
			fmt.Println("Pastry session got:", sess)
		case peer := <-o.peerSink:
			fmt.Println("Pastry boostrap found:", peer)
			// Conenct'em all!!!
			ses, err := session.Dial(peer.IP.String(), peer.Port, o.self, o.lkey, o.rkeys[o.self])
			fmt.Println("Connected to:", ses, err)
		}
	}
}
