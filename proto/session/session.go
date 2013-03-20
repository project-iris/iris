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
package session

import (
	"bytes"
	"config"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"hash"
	"io"
	"proto/stream"
)

type packet struct {
	headers []byte
	payload []byte
}

type Headers struct {
	origin string
	target string

	key []byte
	iv  []byte
	mac []byte
}

// Accomplishes secure and authenticated communication.
type Session struct {
	socket *stream.Stream
	cipher cipher.Stream
	macer  hash.Hash
}

// Creates a new session from the given data stream and negotiated secret. The
// derived cryptographic primitives are configured in the config.go file. Any
// failure here means invalid/corrupt configurations, thus will lead to a panic.
func newSession(strm *stream.Stream, secret []byte) *Session {
	// Create the key derivation function
	hasher := func() hash.Hash { return config.HkdfHash.New() }
	hkdf := hkdf.New(hasher, secret, config.HkdfSalt, config.HkdfInfo)

	// Extract the symmetric key and create the block cipher
	key := make([]byte, config.SesCipherBits/8)
	n, err := io.ReadFull(hkdf, key)
	if n != len(key) || err != nil {
		panic(fmt.Sprintf("Failed to extract session key: %v", err))
	}
	block, err := config.SesCipher(key)
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
	salt := make([]byte, config.SesHash().Size())
	n, err = io.ReadFull(hkdf, salt)
	if n != len(salt) || err != nil {
		panic(fmt.Sprintf("Failed to extract session mac salt: %v", err))
	}
	mac := hmac.New(config.SesHash, salt)

	return &Session{strm, stream, mac}
}

func (s *Session) Send(msg interface{}) (err error) {
	// Close session on any error
	defer func() {
		if err != nil {
			s.socket.Close()
		}
	}()

	head := new(Headers)
	pack := new(packet)

	// Flatten the custom message
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err = enc.Encode(msg)
	if err != nil {
		return
	}
	// Generate a new temporary key, IV and encrypt the message contents
	head.key = make([]byte, config.PackCipherBits/8)
	n, err := io.ReadFull(rand.Reader, head.key)
	if n != len(head.key) || err != nil {
		return
	}
	block, err := config.PackCipher(head.key)
	if err != nil {
		return
	}
	head.iv = make([]byte, block.BlockSize())
	n, err = io.ReadFull(rand.Reader, head.iv)
	if n != len(head.iv) || err != nil {
		return
	}
	stream := cipher.NewCTR(block, head.iv)
	stream.XORKeyStream(pack.payload, buf.Bytes())

	// Generate the MAC of the encrypted payload
	s.macer.Write(pack.payload)
	head.mac = s.macer.Sum(nil)

	// Encrypt the headers
	buf = new(bytes.Buffer)
	enc = gob.NewEncoder(buf)
	err = enc.Encode(head)
	if err != nil {
		return
	}
	s.cipher.XORKeyStream(pack.headers, buf.Bytes())

	return s.socket.Send(pack)
}

func (s *Session) Recv(msg interface{}) (err error) {
	// Close session on any error
	defer func() {
		if err != nil {
			s.socket.Close()
		}
	}()
	// Retrieve a new package
	pack := new(packet)
	err = s.socket.Recv(pack)
	if err != nil {
		return
	}
	// Extract the package headers
	head := new(Headers)
	buf := bytes.NewBuffer(pack.headers)
	dec := gob.NewDecoder(buf)
	err = dec.Decode(head)
	if err != nil {
		return
	}
	// Verify the payload contents
	s.macer.Write(pack.payload)
	if !bytes.Equal(head.mac, s.macer.Sum(nil)) {
		return errors.New(fmt.Sprintf("mac mismatch: have %v, want %v.", s.macer.Sum(nil), head.mac))
	}
	// Decrypt and extract the incoming message
	block, err := config.PackCipher(head.key)
	if err != nil {
		return
	}
	buf = new(bytes.Buffer)
	dec = gob.NewDecoder(buf)
	stream := cipher.NewCTR(block, head.iv)
	stream.XORKeyStream(buf.Bytes(), pack.payload)
	return dec.Decode(msg)
}
