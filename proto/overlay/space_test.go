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
	"math/big"
	"testing"
)

type spaceTest struct {
	idA    *big.Int
	idB    *big.Int
	delta  *big.Int
	dist   *big.Int
	prefix int
	digit  int
}

var one = big.NewInt(1)

// The tests assume the default 128 bit pastry space and 4 bit digits!
var spaceTests = []spaceTest{
	// Simple startup cases
	{big.NewInt(0), big.NewInt(15), big.NewInt(15), big.NewInt(15), config.PastrySpace/config.PastryBase - 1, 15},
	{big.NewInt(15), big.NewInt(0), big.NewInt(-15), big.NewInt(15), config.PastrySpace/config.PastryBase - 1, 0},
	{big.NewInt(0), big.NewInt(127), big.NewInt(127), big.NewInt(127), config.PastrySpace/config.PastryBase - 2, 7},
	{big.NewInt(127), big.NewInt(0), big.NewInt(-127), big.NewInt(127), config.PastrySpace/config.PastryBase - 2, 0},
	{big.NewInt(128), big.NewInt(256), big.NewInt(128), big.NewInt(128), config.PastrySpace/config.PastryBase - 3, 1},
	{big.NewInt(256), big.NewInt(128), big.NewInt(-128), big.NewInt(128), config.PastrySpace/config.PastryBase - 3, 0},

	// Boring cases
	{big.NewInt(65536), big.NewInt(262144), big.NewInt(196608), big.NewInt(196608), config.PastrySpace/config.PastryBase - 5, 4},
	{big.NewInt(262144), big.NewInt(65536), big.NewInt(-196608), big.NewInt(196608), config.PastrySpace/config.PastryBase - 5, 1},

	// Circular wrapping
	{new(big.Int).Sub(modulo, one), big.NewInt(0), big.NewInt(1), big.NewInt(1), 0, 0},
	{big.NewInt(0), new(big.Int).Sub(modulo, one), big.NewInt(-1), big.NewInt(1), 0, 15},
	{new(big.Int).Sub(modulo, one), big.NewInt(1), big.NewInt(2), big.NewInt(2), 0, 0},
	{big.NewInt(1), new(big.Int).Sub(modulo, one), big.NewInt(-2), big.NewInt(2), 0, 15},

	// Half splits
	{big.NewInt(0), posmid, posmid, posmid, 0, 8},
	{posmid, big.NewInt(0), negmid, posmid, 0, 0},
	{big.NewInt(0), new(big.Int).Sub(posmid, one), new(big.Int).Sub(posmid, one), new(big.Int).Sub(posmid, one), 0, 7},
	{new(big.Int).Sub(posmid, one), big.NewInt(0), new(big.Int).Add(negmid, one), new(big.Int).Sub(posmid, one), 0, 0},
	{big.NewInt(0), new(big.Int).Add(posmid, one), new(big.Int).Add(negmid, one), new(big.Int).Sub(posmid, one), 0, 8},
	{new(big.Int).Add(posmid, one), big.NewInt(0), new(big.Int).Sub(posmid, one), new(big.Int).Sub(posmid, one), 0, 0},
}

func TestSpace(t *testing.T) {
	for i, tt := range spaceTests {
		if d := delta(tt.idA, tt.idB); tt.delta.Cmp(d) != 0 {
			t.Errorf("test %d: delta mismatch: have %v, want %v.", i, d, tt.delta)
		}
		if d := distance(tt.idA, tt.idB); tt.dist.Cmp(d) != 0 {
			t.Errorf("test %d: dist mismatch: have %v, want %v.", i, d, tt.dist)
		}
		if p, d := prefix(tt.idA, tt.idB); tt.prefix != p || tt.digit != d {
			t.Errorf("test %d: prefix/digit mismatch: have %v/%v, want %v/%v.", i, p, d, tt.prefix, tt.digit)
		}
	}
}
