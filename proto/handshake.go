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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sts"
	"fmt"
	"hash"
	"io"
	"log"
	"proto/stream"
)

func Listen(port int, key *rsa.PrivateKey, store map[string]*rsa.PublicKey) (chan *Session, chan struct{}, error) {
	// Open the TCP socket
	netSink, netQuit, err := stream.Listen(port)
	if err != nil {
		return nil, nil, err
	}
	// For each incoming connection, execute auth negotiation
	sink := make(chan *Session)
	quit := make(chan struct{})
	go accept(key, store, sink, quit, netSink, netQuit)
	return sink, quit, nil
}

func Dial(host string, port int, self string, skey *rsa.PrivateKey, pkey *rsa.PublicKey) (*Session, error) {
	// Open the TCP socket
	conn, err := stream.Dial(host, port)
	if err != nil {
		return nil, err
	}
	return connect(conn, self, skey, pkey)
}

// Accepts incoming net connections and initiates an STS authentication for each of them. Those that
// successfully pass the protocol get sent back on the session channel.
func accept(key *rsa.PrivateKey, store map[string]*rsa.PublicKey, sink chan *Session, quit chan struct{},
	netSink chan *stream.Stream, netQuit chan struct{}) {
	for {
		select {
		case <-quit:
			close(netQuit)
			return
		case conn, ok := <-netSink:
			// Negotiate an STS session (if channel has not been closed)
			if !ok {
				return
			}
			go authenticate(conn, key, store, sink)
		}
	}
}

func connect(strm *stream.Stream, self string, skey *rsa.PrivateKey, pkey *rsa.PublicKey) (ses *Session, err error) {
	// Defer an error handler that will ensure a closed stream
	defer func() {
		if err != nil {
			strm.Close()
			ses = nil
		}
	}()
	// Create a new empty session
	session, err := sts.New(rand.Reader, stsGroup, stsGenerator, stsCipher, stsCipherBits, stsSigHash)
	if err != nil {
		log.Printf("failed to create new session: %v\n", err)
		return
	}
	// Initiate a key exchange, send the exponential
	exp, err := session.Initiate()
	if err != nil {
		log.Printf("failed to initiate key exchange: %v\n", err)
		return
	}
	err = strm.Send(authRequest{self, exp})
	if err != nil {
		log.Printf("failed to send auth request: %v\n", err)
		return
	}
	// Receive the foreign exponential and auth token and if verifies, send own auth
	chall := new(authChallenge)
	err = strm.Recv(chall)
	if err != nil {
		log.Printf("failed to receive auth challenge: %v\n", err)
		return
	}
	token, err := session.Verify(rand.Reader, skey, pkey, chall.Exp, chall.Token)
	if err != nil {
		log.Printf("failed to verify acceptor auth token: %v\n", err)
		return
	}
	err = strm.Send(authResponse{token})
	if err != nil {
		log.Printf("failed to send auth response: %v\n", err)
		return
	}
	// Protocol done, other side should finalize if all is correct
	secret, err := session.Secret()
	if err != nil {
		log.Printf("failed to retrieve exchanged secret: %v\n", err)
		return
	}
	return newSession(strm, secret), nil
}

func authenticate(strm *stream.Stream, key *rsa.PrivateKey, store map[string]*rsa.PublicKey, sink chan *Session) {
	// Defer an error handler that will ensure a closed stream
	var err error
	defer func() {
		if err != nil {
			strm.Close()
		}
	}()
	// Create a new STS session
	session, err := sts.New(rand.Reader, stsGroup, stsGenerator, stsCipher, stsCipherBits, stsSigHash)
	if err != nil {
		log.Printf("failed to create new session: %v\n", err)
		return
	}
	// Receive foreign exponential, accept the incoming key exchange request and send back own exp + auth token
	req := new(authRequest)
	err = strm.Recv(req)
	if err != nil {
		log.Printf("failed to decode auth request: %v\n", err)
		return
	}
	exp, token, err := session.Accept(rand.Reader, key, req.Exp)
	if err != nil {
		log.Printf("failed to accept incoming exchange: %v\n", err)
		return
	}
	err = strm.Send(authChallenge{exp, token})
	if err != nil {
		log.Printf("failed to encode auth challenge: %v\n", err)
		return
	}
	// Receive the foreign auth token and if verifies conclude session
	resp := new(authResponse)
	err = strm.Recv(resp)
	if err != nil {
		log.Printf("failed to decode auth response: %v\n", err)
		return
	}
	err = session.Finalize(store[req.Id], resp.Token)
	if err != nil {
		log.Printf("failed to finalize exchange: %v\n", err)
		return
	}
	// Protocol done
	secret, err := session.Secret()
	if err != nil {
		log.Printf("failed to retrieve exchanged secret: %v\n", err)
		return
	}
	sink <- newSession(strm, secret)
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

	return &Session{*strm, stream, mac}
}
