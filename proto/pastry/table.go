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

// Contains the routing table definition and a handful of utility functions.

package pastry

import (
	"math/big"

	"github.com/project-iris/iris/config"
)

// Simplified Pastry routing table.
type table struct {
	leaves []*big.Int
	routes [][]*big.Int
}

// Creates a new empty routing table.
func newRoutingTable(origin *big.Int) *table {
	res := new(table)

	// Create the leaf set with only the origin point inside
	res.leaves = make([]*big.Int, 1, config.PastryLeaves)
	res.leaves[0] = origin

	// Create the empty routing table of predefined size
	res.routes = make([][]*big.Int, config.PastrySpace/config.PastryBase)
	for i := 0; i < len(res.routes); i++ {
		res.routes[i] = make([]*big.Int, 1<<uint(config.PastryBase))
	}
	return res
}

// Creates a copy of the routing table
func (t *table) copy() *table {
	res := new(table)

	// Copy the leafset
	res.leaves = make([]*big.Int, len(t.leaves), config.PastryLeaves)
	copy(res.leaves, t.leaves)

	// Copy the routing table
	res.routes = make([][]*big.Int, len(t.routes))
	for i := 0; i < len(res.routes); i++ {
		res.routes[i] = make([]*big.Int, len(t.routes[i]))
		copy(res.routes[i], t.routes[i])
	}
	return res
}
