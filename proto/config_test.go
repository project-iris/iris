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
package proto

import (
	"bytes"
	"crypto/rand"
	"io"
	"math/big"
	"testing"
)

func TestConfigSts(t *testing.T) {
	// Ensure cyclic group is safe-prime and a valid (big) generator
	negligibleExp := 82
	if !stsGroup.ProbablyPrime(negligibleExp / 2) {
		t.Errorf("proto config (sts): non-prime cyclic group: %v.", stsGroup)
	}
	q := new(big.Int).Sub(stsGroup, big.NewInt(1))
	q = new(big.Int).Div(q, big.NewInt(2))
	if !q.ProbablyPrime(negligibleExp / 2) {
		t.Errorf("proto config (sts): non-safe-prime base: %v = 2*%v + 1.", stsGroup, q)
	}
	if new(big.Int).Exp(stsGenerator, q, stsGroup).Cmp(big.NewInt(1)) != 0 {
		t.Errorf("proto config (sts): invalid generator: %v.", stsGenerator)
	}
	// Ensure the cipher and key size combination is valid
	key := make([]byte, stsCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("proto config (sts): failed to generate random key: %v.", err)
	}
	_, err = stsCipher(key)
	if err != nil {
		t.Errorf("proto config (sts): failed to create requested cipher: %v.", err)
	}
	// Ensure the hash is linked to the binary
	if !stsSigHash.Available() {
		t.Errorf("proto config (sts): requested hash not linked into binary.")
	}
}

func TestConfigHkdf(t *testing.T) {
	// Ensure the hash is linked to the binary
	if !hkdfHash.Available() {
		t.Errorf("proto config (hkdf): requested hash not linked into binary.")
	}
	// Ensure the salt and info fields are valid and unique (only an extra safety measure)
	if hkdfSalt == nil {
		t.Errorf("proto config (hkdf): salt shouldn't be empty.")
	}
	if hkdfInfo == nil {
		t.Errorf("proto config (hkdf): info shouldn't be empty.")
	}
	if bytes.Equal(hkdfSalt, hkdfInfo) {
		t.Errorf("proto config (hkdf): salt and info fields should be unique.")
	}
}

func TestConfigSession(t *testing.T) {
	// Ensure a valid symmetric cipher
	key := make([]byte, sesCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("proto config (session): failed to generate random key: %v.", err)
	}
	_, err = sesCipher(key)
	if err != nil {
		t.Errorf("proto config (session): failed to create requested cipher: %v.", err)
	}
}

func TestConfigPack(t *testing.T) {
	// Ensure a valid symmetric cipher
	key := make([]byte, packCipherBits/8)
	n, err := io.ReadFull(rand.Reader, key)
	if n != len(key) || err != nil {
		t.Errorf("proto config (pack): failed to generate random key: %v.", err)
	}
	_, err = packCipher(key)
	if err != nil {
		t.Errorf("proto config (pack): failed to create requested cipher: %v.", err)
	}
}
