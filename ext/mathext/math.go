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

// Package mathext contains extensions to the base Go math package.
package mathext

import (
	"math/big"
)

// Returns the larger of x or y.
func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// Returns the larger of x or y.
func MaxBigInt(x, y *big.Int) *big.Int {
	if x.Cmp(y) > 0 {
		return x
	}
	return y
}

// Returns the larger of x or y.
func MaxBigRat(x, y *big.Rat) *big.Rat {
	if x.Cmp(y) > 0 {
		return x
	}
	return y
}

// Returns the smaller of x or y.
func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Returns the smaller of x or y.
func MinBigInt(x, y *big.Int) *big.Int {
	if x.Cmp(y) < 0 {
		return x
	}
	return y
}

// Returns the smaller of x or y.
func MinBigRat(x, y *big.Rat) *big.Rat {
	if x.Cmp(y) < 0 {
		return x
	}
	return y
}
