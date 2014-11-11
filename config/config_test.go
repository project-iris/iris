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
	if n, err := io.ReadFull(rand.Reader, key); n != len(key) || err != nil {
		t.Errorf("config (sts): failed to generate random key: %v.", err)
	}
	if _, err := StsCipher(key); err != nil {
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
	key := make([]byte, SessionCipherBits/8)
	if n, err := io.ReadFull(rand.Reader, key); n != len(key) || err != nil {
		t.Errorf("config (session): failed to generate random key: %v.", err)
	}
	if _, err := SessionCipher(key); err != nil {
		t.Errorf("config (session): failed to create requested cipher: %v.", err)
	}
	// Ensure a valid MAC hash
	if SessionHash() == nil {
		t.Fatalf("config (session): failed to create requested hasher.")
	}
}

func TestPack(t *testing.T) {
	// Ensure a valid symmetric cipher
	key := make([]byte, PacketCipherBits/8)
	if n, err := io.ReadFull(rand.Reader, key); n != len(key) || err != nil {
		t.Errorf("config (packet): failed to generate random key: %v.", err)
	}
	if _, err := PacketCipher(key); err != nil {
		t.Errorf("config (packet): failed to create requested cipher: %v.", err)
	}
}

func TestPastry(t *testing.T) {
	// Ensure pastry space is reduced size (at least issue a warning)
	if PastrySpace != 40 {
		t.Errorf("config (overlay): address space is invalid: have %v, want %v.", PastrySpace, 40)
	}
	if size := PastryResolver().Size() * 8; size < PastrySpace {
		t.Errorf("config (overlay): resolver does not output enough bits for space: have %v, want %v.", size, PastrySpace)
	}
	// Do some sanity checks on the parameters
	if PastryBase < 1 {
		t.Errorf("config (overlay): invalid base bits: have %v, want min 1.", PastryBase)
	}
	if PastrySpace%PastryBase != 0 {
		t.Errorf("config (overlay): address space is not divisible into bases: %v %% %v != 0", PastrySpace, PastryBase)
	}
	if PastryLeaves != 1<<uint(PastryBase-1) && PastryLeaves != 1<<uint(PastryBase) {
		t.Errorf("config (overlay): invalid leave set size: have %v, want %v or %v.", PastryLeaves, 1<<uint(PastryBase-1), 1<<uint(PastryBase))
	}
	// Make some trivial checks for the tuning parameters
	if PastryNetBuffer < 16 || PastryNetBuffer > 128 {
		t.Errorf("config (overlay): strange network buffer size: have %v, want from [16..128].", PastryNetBuffer)
	}
}
