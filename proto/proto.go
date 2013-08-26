// Iris - Decentralized Messaging Framework
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

// Package proto contains the baseline message container and the endpoint crypto
// methods.
package proto

import (
	"crypto/cipher"
	"crypto/rand"
	"github.com/karalabe/iris/config"
	"io"
)

// Baseline message headers.
type Header struct {
	Meta interface{} // Metadata usable by upper network layers
	Key  []byte      // AES key if the payload is encrypted (nil otherwise)
	Iv   []byte      // Counter mode nonce if the payload is encrypted (nil otherwise)
}

// Iris message consisting of the payload and attached headers.
type Message struct {
	Head Header // Baseline headers
	Data []byte // Payload in plain or ciphertext form
}

// Encrypts a plaintext message with a temporary key and IV.
func (m *Message) Encrypt() error {
	// Generate a new temporary key and the associated block cipher
	key := make([]byte, config.PacketCipherBits/8)
	if n, err := io.ReadFull(rand.Reader, key); n != len(key) || err != nil {
		return err
	}
	block, err := config.PacketCipher(key)
	if err != nil {
		return err
	}
	// Generate a new random counter mode IV and the associated stream cipher
	iv := make([]byte, block.BlockSize())
	if n, err := io.ReadFull(rand.Reader, iv); n != len(iv) || err != nil {
		return err
	}
	stream := cipher.NewCTR(block, iv)

	// Encrypt the message, save the nonces and return
	stream.XORKeyStream(m.Data, m.Data)
	m.Head.Key = key
	m.Head.Iv = iv
	return nil
}

// Decrypts a ciphertext message using the given key and IV.
func (m *Message) Decrypt() error {
	// Create the stream cipher for decryption
	block, err := config.PacketCipher(m.Head.Key)
	if err != nil {
		return err
	}
	stream := cipher.NewCTR(block, m.Head.Iv)

	// Decrypt the message, clear out the crypto headers and return
	stream.XORKeyStream(m.Data, m.Data)
	m.Head.Key = nil
	m.Head.Iv = nil
	return nil
}
