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

package cyclic

import (
	"crypto/rand"
	"math/big"
	"testing"
)

var cyclicTests = []int{128, 256, 314}

func TestCyclic(t *testing.T) {
	for i, tt := range cyclicTests {
		group, err := New(rand.Reader, tt)
		if err != nil {
			t.Errorf("test %d: error returned: %v.", i, err)
		}
		if group.Base.BitLen() < tt {
			t.Errorf("test %d: insecure base order: have %v, want %v.", i, group.Base.BitLen(), tt)
		}
		if !group.Base.ProbablyPrime(negligibleExp / 2) {
			t.Errorf("test %d: non-prime base: %v.", i, group.Base)
		}
		q := new(big.Int).Sub(group.Base, big.NewInt(1))
		q = new(big.Int).Div(q, big.NewInt(2))
		if !q.ProbablyPrime(negligibleExp / 2) {
			t.Errorf("test %d: non-safe-prime base: %v = 2*%v + 1.", i, group.Base, q)
		}
		if new(big.Int).Exp(group.Generator, q, group.Base).Cmp(big.NewInt(1)) != 0 {
			t.Errorf("test %d: invalid generator: %v.", i, group.Generator)
		}
	}
}
