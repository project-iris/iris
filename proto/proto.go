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

// Package proto contains the baseline message container and the endpoint crypto
// methods.
package proto

import (
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/project-iris/iris/config"
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

	secure bool // Flag specifying whether the data segment was encrypted or not
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

	m.secure = true
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

// Internal, used by the link package to verify security.
func (m *Message) Secure() bool {
	return m.secure
}

// Internal, used by the link package to assert known security.
func (m *Message) KnownSecure() {
	m.secure = true
}
