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
package config

import (
	"bytes"
	"crypto/rand"
	"io"
	"math/big"
	"testing"
)

func TestSts(t *testing.T) {
	// Ensure cyclic group is safe-prime and a valid (big) generator
	negligibleExp := 82
	if !StsGroup.ProbablyPrime(negligibleExp / 2) {
		t.Errorf("config (sts): non-prime cyclic group: %v.", StsGroup)
	}
	q := new(big.Int).Sub(StsGroup, big.NewInt(1))
	q = new(big.Int).Div(q, big.NewInt(2))
	if !q.ProbablyPrime(negligibleExp / 2) {
		t.Errorf("config (sts): non-safe-prime base: %v = 2*%v + 1.", StsGroup, q)
	}
	if new(big.Int).Exp(StsGenerator, q, StsGroup).Cmp(big.NewInt(1)) != 0 {
		t.Errorf("config (sts): invalid generator: %v.", StsGenerator)
	}
	// Ensure the cipher and key size combination is valid
	key := make([]byte, StsCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("config (sts): failed to generate random key: %v.", err)
	}
	_, err = StsCipher(key)
	if err != nil {
		t.Errorf("config (sts): failed to create requested cipher: %v.", err)
	}
	// Ensure the hash is linked to the binary
	if !StsSigHash.Available() {
		t.Errorf("config (sts): requested hash not linked into binary.")
	}
}

func TestHkdf(t *testing.T) {
	// Ensure the hash is linked to the binary
	if !HkdfHash.Available() {
		t.Errorf("config (hkdf): requested hash not linked into binary.")
	}
	// Ensure the salt and info fields are valid and unique (only an extra safety measure)
	if HkdfSalt == nil {
		t.Errorf("config (hkdf): salt shouldn't be empty.")
	}
	if HkdfInfo == nil {
		t.Errorf("config (hkdf): info shouldn't be empty.")
	}
	if bytes.Equal(HkdfSalt, HkdfInfo) {
		t.Errorf("config (hkdf): salt and info fields should be unique.")
	}
}

func TestSession(t *testing.T) {
	// Ensure a valid symmetric cipher
	key := make([]byte, SesCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("config (session): failed to generate random key: %v.", err)
	}
	_, err = SesCipher(key)
	if err != nil {
		t.Errorf("config (session): failed to create requested cipher: %v.", err)
	}
}

func TestPack(t *testing.T) {
	// Ensure a valid symmetric cipher
	key := make([]byte, PackCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("config (pack): failed to generate random key: %v.", err)
	}
	_, err = PackCipher(key)
	if err != nil {
		t.Errorf("config (pack): failed to create requested cipher: %v.", err)
	}
}
