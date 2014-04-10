// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

// This file contains the entity definition and some helper funcs, separated to
// leave the main logic clearer.

package balancer

import (
	"math/big"
	"sort"
)

// Entity and related information.
type entity struct {
	id  *big.Int // Unique identifier of the entity
	cap int      // Message capacity as reported by entity
}

// Entity slice implementing sort.Interface.
type entitySlice []*entity

// Required for sort.Sort.
func (s entitySlice) Len() int {
	return len(s)
}

// Required for sort.Sort.
func (s entitySlice) Less(i, j int) bool {
	return s[i].id.Cmp(s[j].id) < 0
}

// Required for sort.Sort.
func (s entitySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Searches the slice for an id and returns accordig to sort.Search.
func (s entitySlice) Search(x *big.Int) int {
	return sort.Search(len(s), func(i int) bool { return s[i].id.Cmp(x) >= 0 })
}
