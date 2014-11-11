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

// Package sortext contains extensions to the base Go sort package.
package sortext

import (
	"math/big"
	"sort"
)

// BigIntSlice attaches the methods of Interface to []*big.Int, sorting in increasing order.
type BigIntSlice []*big.Int

func (b BigIntSlice) Len() int           { return len(b) }
func (b BigIntSlice) Less(i, j int) bool { return b[i].Cmp(b[j]) < 0 }
func (b BigIntSlice) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Sort is a convenience method.
func (b BigIntSlice) Sort() { sort.Sort(b) }

// BigRatSlice attaches the methods of Interface to []*big.Rat, sorting in increasing order.
type BigRatSlice []*big.Rat

func (b BigRatSlice) Len() int           { return len(b) }
func (b BigRatSlice) Less(i, j int) bool { return b[i].Cmp(b[j]) < 0 }
func (b BigRatSlice) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

// Sort is a convenience method.
func (b BigRatSlice) Sort() { sort.Sort(b) }

// BigInts sorts a slice of *big.Ints in increasing order.
func BigInts(a []*big.Int) { sort.Sort(BigIntSlice(a)) }

// BigRats sorts a slice of *big.Rats in increasing order.
func BigRats(a []*big.Rat) { sort.Sort(BigRatSlice(a)) }

// BigIntsAreSorted tests whether a slice of *big.Ints is sorted in increasing order.
func BigIntsAreSorted(a []*big.Int) bool { return sort.IsSorted(BigIntSlice(a)) }

// BigRatsAreSorted tests whether a slice of *big.Rats is sorted in increasing order.
func BigRatsAreSorted(a []*big.Rat) bool { return sort.IsSorted(BigRatSlice(a)) }
