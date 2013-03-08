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
package hkdf_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"iris/crypto/hkdf"
)

// Usage example that expands one master key into three other cryptographically
// secure keys.
func Example_usage() {
	// Underlying hash function to use
	hash := sha1.New

	// Cryptographically secure - secret - master key
	master := []byte{0x00, 0x01, 0x02, 0x03} // i.e. NOT this

	// Non secret salt, optional (can be nil)
	// Recommended: hash-length sized random
	salt := make([]byte, hash().Size())
	n, err := io.ReadFull(rand.Reader, salt)
	if n != len(salt) || err != nil {
		fmt.Println("error:", err)
		return
	}

	// Non secret context specific info, optional (can be nil)
	// Note, independent from the master key
	info := []byte{0x03, 0x14, 0x15, 0x92, 0x65} // I like pie

	// Create the key derivation function
	hkdf := hkdf.New(hash, master, salt, info)

	// Generate the required keys
	keys := make([][]byte, 3)
	for i := 0; i < len(keys); i++ {
		keys[i] = make([]byte, 24)
		n, err := io.ReadFull(hkdf, keys[i])
		if n != len(keys[i]) || err != nil {
			fmt.Println("error:", err)
			return
		}
	}

	// Keys should contain 192 bit random keys
	for i := 1; i <= len(keys); i++ {
		fmt.Printf("Key #%d: %v\n", i, !bytes.Equal(keys[i-1], make([]byte, 24)))
	}

	// Output:
	// Key #1: true
	// Key #2: true
	// Key #3: true
}
