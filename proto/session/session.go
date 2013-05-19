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
	"crypto/cipher"
	"crypto/hmac"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/crypto/hkdf"
	"github.com/karalabe/iris/proto/stream"
	"hash"
	"io"
	"log"
	"net"
)

// Structure containing the message headers:
//  - metadata or app specific headers
//  - sender and recipient
//  - symmetric key and ctr iv used to encrypt the payload
//  - mac of the encrypted payload (internal)
type Header struct {
	Meta interface{}
	Key  []byte
	Iv   []byte
	Mac  []byte
}

// Simple container for the header, data and metadata to be able to pass them
// around together.
type Message struct {
	Head Header
	Data []byte
}

// Accomplishes secure and authenticated full duplex communication.
type Session struct {
	socket *stream.Stream

	inCipher  cipher.Stream
	outCipher cipher.Stream

	inMacer  hash.Hash
	outMacer hash.Hash

	inBuffer  bytes.Buffer
	outBuffer bytes.Buffer

	inCoder  *gob.Decoder
	outCoder *gob.Encoder
}

// Creates a new, full-duplex session from the given data stream and negotiated
// secret. The initiator is used to decide the key derivation order for the two
// half-duplex channels.
//
// Note, the derived cryptographic primitives are configured in config.go. Any
// failure here means invalid/corrupt configurations, thus will lead to a panic.
func newSession(strm *stream.Stream, secret []byte, initiator bool) *Session {
	ses := new(Session)
	ses.socket = strm

	// Create the key derivation function
	hasher := func() hash.Hash { return config.HkdfHash.New() }
	hkdf := hkdf.New(hasher, secret, config.HkdfSalt, config.HkdfInfo)

	// Create the duplex channel
	sc, sm := makeHalfDuplex(hkdf)
	cc, cm := makeHalfDuplex(hkdf)
	if initiator {
		ses.inCipher, ses.outCipher, ses.inMacer, ses.outMacer = cc, sc, cm, sm
	} else {
		ses.inCipher, ses.outCipher, ses.inMacer, ses.outMacer = sc, cc, sm, cm
	}
	// Create the two en/de coders for the header
	ses.inCoder = gob.NewDecoder(&ses.inBuffer)
	ses.outCoder = gob.NewEncoder(&ses.outBuffer)
	return ses
}

// Assembles the crypto primitives needed for a one way communication channel:
// the stream cipher for encryption and the mac for authentication.
func makeHalfDuplex(hkdf io.Reader) (cipher.Stream, hash.Hash) {
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

	return stream, mac
}

// Starts the session data transfer from stream to app (sink) and app to stream
// (returned channel).
func (s *Session) Communicate(sink chan *Message, quit chan struct{}) chan *Message {
	ch := make(chan *Message, cap(sink))
	go s.sender(ch, quit)
	go s.receiver(sink)
	return ch
}

// Retrieves the raw connection object if special manipulations are needed.
func (s *Session) Raw() *net.TCPConn {
	return s.socket.Raw()
}

// Sends messages from the upper layers into the session stream.
func (s *Session) sender(net chan *Message, quit chan struct{}) {
	defer s.socket.Close()
	for {
		select {
		case <-quit:
			return
		case msg, ok := <-net:
			if !ok {
				return
			}
			if err := s.send(msg); err != nil {
				return
			}
		}
	}
}

// The actual message sending logic. Calculates the payload mac, encrypts the
// headers and sends it down to the stream.
func (s *Session) send(msg *Message) (err error) {
	// Generate the MAC of the encrypted payload
	s.outMacer.Write(msg.Data)
	msg.Head.Mac = s.outMacer.Sum(nil)

	// Flatten and encrypt the headers
	if err = s.outCoder.Encode(msg.Head); err != nil {
		log.Printf("failed to encode header %v: %v", msg.Head, err)
		return
	}
	s.outCipher.XORKeyStream(s.outBuffer.Bytes(), s.outBuffer.Bytes())

	// Send the multipart message (headers + payload)
	err = s.socket.Send(s.outBuffer.Bytes())
	if err != nil {
		return
	}
	s.outBuffer.Reset()
	return s.socket.Send(msg.Data)
}

// Transfers messages from the session to the upper layers decoding the headers.
// The method will finish on either an error or a close (remote or sender func).
func (s *Session) receiver(app chan *Message) {
	defer close(app)
	for {
		msg, err := s.recv()
		if err != nil {
			return
		}
		app <- msg
	}
}

// The actual message receiving logic. Reads a message from the stream, verifies
// its mac, decodes the headers and send it upwards.
func (s *Session) recv() (msg *Message, err error) {
	// Close session on any error
	defer func() {
		if err != nil {
			s.socket.Close()
			msg = nil
		}
	}()
	msg = new(Message)

	// Retrieve a new package
	var input []byte
	if err = s.socket.Recv(&input); err != nil {
		return
	}
	if err = s.socket.Recv(&msg.Data); err != nil {
		return
	}
	// Extract the package contents
	s.inCipher.XORKeyStream(input, input)
	s.inBuffer.Write(input)
	if err = s.inCoder.Decode(&msg.Head); err != nil {
		return
	}
	// Verify the payload contents
	s.inMacer.Write(msg.Data)
	if !bytes.Equal(msg.Head.Mac, s.inMacer.Sum(nil)) {
		err = errors.New(fmt.Sprintf("mac mismatch: have %v, want %v.", s.inMacer.Sum(nil), msg.Head.Mac))
		return
	}
	return msg, nil
}
