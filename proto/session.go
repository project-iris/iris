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
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"fmt"
	"hash"
	"io"
	"proto/stream"
)

// Accomplishes secure and authenticated communication.
type Session struct {
	socket *stream.Stream
	cipher cipher.Stream
	mac    hash.Hash
}

// Creates a new session from the given data stream and negotiated secret. The
// derived cryptographic primitives are configured in the config.go file. Any
// failure here means invalid/corrupt configurations, thus will lead to a panic.
func newSession(strm *stream.Stream, secret []byte) *Session {
	// Create the key derivation function
	hasher := func() hash.Hash { return hkdfHash.New() }
	hkdf := hkdf.New(hasher, secret, hkdfSalt, hkdfInfo)

	// Extract the symmetric key and create the block cipher
	key := make([]byte, sesCipherBits/8)
	n, err := io.ReadFull(hkdf, key)
	if n != len(key) || err != nil {
		panic(fmt.Sprintf("Failed to extract session key: %v", err))
	}
	block, err := sesCipher(key)
	if err != nil {
		panic(fmt.Sprintf("Failed to create session cipher: %v", err))
	}
	// Extract the IV for the counter mode and create the stream cipher
	iv := make([]byte, block.BlockSize())
	n, err = io.ReadFull(hkdf, iv)
	if n != len(iv) || err != nil {
		panic(fmt.Sprintf("Failed to extract session IV: %v", err))
	}
	stream := cipher.NewCTR(block, iv)

	// Extract the HMAC key and create the session MACer
	salt := make([]byte, sesHash().Size())
	n, err = io.ReadFull(hkdf, salt)
	if n != len(salt) || err != nil {
		panic(fmt.Sprintf("Failed to extract session mac salt: %v", err))
	}
	mac := hmac.New(sesHash, salt)

	return &Session{strm, stream, mac}
}
