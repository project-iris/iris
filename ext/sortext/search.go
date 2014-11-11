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

package sortext

import (
	"math/big"
	"sort"
)

// SearchBigInts searches for x in a sorted slice of *big.Ints and returns the
// index as specified by Search. The return value is the index to insert x if x
// is not present (it could be len(a)).
// The slice must be sorted in ascending order.
func SearchBigInts(a []*big.Int, x *big.Int) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Cmp(x) >= 0 })
}

// SearchBigRats searches for x in a sorted slice of *big.Rats and returns the
// index as specified by Search. The return value is the index to insert x if x
// is not present (it could be len(a)).
// The slice must be sorted in ascending order.
func SearchBigRats(a []*big.Rat, x *big.Rat) int {
	return sort.Search(len(a), func(i int) bool { return a[i].Cmp(x) >= 0 })
}

// Search returns the result of applying SearchBigInts to the receiver and x.
func (p BigIntSlice) Search(x *big.Int) int { return SearchBigInts(p, x) }

// Search returns the result of applying SearchBigRats to the receiver and x.
func (p BigRatSlice) Search(x *big.Rat) int { return SearchBigRats(p, x) }
