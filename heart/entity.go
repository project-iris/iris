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

// This file contains the entity definition and some helper funcs, separated to
// leave the main logic clearer.

package heart

import (
	"math/big"
	"sort"
)

// Entity and related information.
type entity struct {
	id   *big.Int // Unique identifier of the entity
	tick int      // Tick of the last recorded activity
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
