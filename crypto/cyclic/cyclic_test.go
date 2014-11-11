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
