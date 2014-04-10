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

package mathext

import (
	"math/big"
	"testing"
)

type wrapperTest struct {
	name string
	res  int
	good int
}

func TestInt(t *testing.T) {
	if m := MaxInt(-10, 10); m != 10 {
		t.Errorf("max mismatch: have %v, want %v.", m, 10)
	}
	if m := MaxInt(10, -10); m != 10 {
		t.Errorf("max mismatch: have %v, want %v.", m, 10)
	}
	if m := MinInt(-10, 10); m != -10 {
		t.Errorf("min mismatch: have %v, want %v.", m, -10)
	}
	if m := MinInt(-10, 10); m != -10 {
		t.Errorf("min mismatch: have %v, want %v.", m, -10)
	}
}

func TestBigInt(t *testing.T) {
	pos := big.NewInt(10)
	neg := big.NewInt(10)

	if m := MaxBigInt(neg, pos); m.Cmp(pos) != 0 {
		t.Errorf("max mismatch: have %v, want %v.", m, pos)
	}
	if m := MaxBigInt(pos, neg); m.Cmp(pos) != 0 {
		t.Errorf("max mismatch: have %v, want %v.", m, pos)
	}
	if m := MinBigInt(neg, pos); m.Cmp(neg) != 0 {
		t.Errorf("min mismatch: have %v, want %v.", m, neg)
	}
	if m := MinBigInt(neg, pos); m.Cmp(neg) != 0 {
		t.Errorf("min mismatch: have %v, want %v.", m, neg)
	}
}

func TestBigRat(t *testing.T) {
	pos := big.NewRat(10, 314)
	neg := big.NewRat(10, 314)

	if m := MaxBigRat(neg, pos); m.Cmp(pos) != 0 {
		t.Errorf("max mismatch: have %v, want %v.", m, pos)
	}
	if m := MaxBigRat(pos, neg); m.Cmp(pos) != 0 {
		t.Errorf("max mismatch: have %v, want %v.", m, pos)
	}
	if m := MinBigRat(neg, pos); m.Cmp(neg) != 0 {
		t.Errorf("min mismatch: have %v, want %v.", m, neg)
	}
	if m := MinBigRat(neg, pos); m.Cmp(neg) != 0 {
		t.Errorf("min mismatch: have %v, want %v.", m, neg)
	}
}
