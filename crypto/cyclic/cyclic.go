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

// Package cyclic generates cryptographic cyclic groups and generators with a
// specified bit-length.
//
// The package is not used in a live system, just to generate an initial config.
//
// Reference: http://goo.gl/zU4ZY
package cyclic

import (
	"crypto/rand"
	"io"
	"math/big"
)

// Cryptographically negligible exponent (2^exp)
var negligibleExp = 82

// Cyclic group with a safe-prime base.
type Group struct {
	Base      *big.Int
	Generator *big.Int
}

// Generate a new cyclic group and generator of given bits size.
func New(random io.Reader, bits int) (*Group, error) {
	for {
		// Generate a large prime of size 'bits'-1
		q, err := rand.Prime(random, bits-1)
		if err != nil {
			return nil, err
		}
		// Calculate the safe prime p=2q+1 of order 'bits'
		p := new(big.Int).Mul(q, big.NewInt(2))
		p = new(big.Int).Add(p, big.NewInt(1))

		// Probability of p being non-prime is negligible
		if p.ProbablyPrime(negligibleExp / 2) {
			for {
				// Generate a generator of p
				a, err := rand.Int(random, p)
				if err != nil {
					return nil, err
				}
				// Ensure generator order is not 2 (efficiency)
				if b := new(big.Int).Exp(a, big.NewInt(2), p); b.Cmp(big.NewInt(1)) == 0 {
					continue
				}
				// Return if generator order is q
				if b := new(big.Int).Exp(a, q, p); b.Cmp(big.NewInt(1)) == 0 {
					return &Group{p, a}, nil
				}
			}
		}
	}
}
