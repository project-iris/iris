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

// Author: peterke@gmail.com (Peter Szilagyi)

// Package hkdf implements the HMAC-based Extract-and-Expand Key Derivation
// Function (HKDF) as defined in Internet Engineering Task Force, Request for
// Comments 5869.
//
// An HKDF is a cryptographic key derivation function (KDF) with the goal of
// expanding some limited size input keying material into one or more
// cryptographically strong secret keys.
//
// RFC 5869: https://tools.ietf.org/html/rfc5869
package hkdf

import (
	"crypto/hmac"
	"errors"
	"fmt"
	"hash"
	"io"
)

type hkdf struct {
	hash func() hash.Hash
	size int

	prk     []byte
	info    []byte
	counter byte

	prev  []byte
	cache []byte
}

func (f *hkdf) Read(p []byte) (n int, err error) {
	need := len(p)
	remains := len(f.cache) + int(255-f.counter+1)*f.size
	if remains < need {
		return 0, errors.New(fmt.Sprintf("entropy limit reached %d < %d", remains, need))
	}

	for len(f.cache) < need {
		input := append(f.prev, append(f.info, f.counter)...)

		expander := hmac.New(f.hash, f.prk)
		expander.Write(input)
		output := expander.Sum(nil)

		f.cache = append(f.cache, output...)
		f.prev = output
		f.counter++
	}

	copy(p, f.cache[:need])
	f.cache = f.cache[need:]

	return need, nil
}

// New returns a new HKDF using the given hash, the master keying material to expand
// and optional salt and info fields.
func New(hash func() hash.Hash, master []byte, salt []byte, info []byte) io.Reader {
	extractor := hmac.New(hash, salt)
	extractor.Write(master)

	return &hkdf{hash, hash().Size(), extractor.Sum(nil), info, 1, []byte{}, []byte{}}
}
