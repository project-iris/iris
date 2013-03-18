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
	_ "crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sts"
	"log"
	"proto/stream"
)

type Session struct {
	stream.Stream

	master []byte
}

func Listen(port int, key *rsa.PrivateKey) (chan int, chan *Session, error) {
	// Open the TCP socket
	netOps, netSink, err := stream.Listen(port)
	if err != nil {
		return nil, nil, err
	}
	// For each incoming connection, execute auth negotiation
	ops := make(chan int)
	sink := make(chan *Session)
	go accept(key, ops, sink, netOps, netSink)
	return ops, sink, nil
}

func Dial(host string, port int, self []byte, skey *rsa.PrivateKey, pkey *rsa.PublicKey) (*Session, error) {
	// Open the TCP socket
	conn, err := stream.Dial(host, port)
	if err != nil {
		return nil, err
	}
	return connect(conn, self, skey, pkey)
}

// Accepts incoming net connections and initiates an STS authentication for each of them. Those that
// successfully pass the protocol get sent back on the session channel.
func accept(key *rsa.PrivateKey, ops chan int, sink chan *Session, netOps chan int, netSink chan *stream.Stream) {
	for {
		select {
		case msg := <-ops:
			// Process any control messages (exit for the moment)
			netOps <- msg
			return
		case conn, ok := <-netSink:
			// Negotiate an STS session (if channel has not been closed)
			if !ok {
				return
			}
			go authenticate(conn, key, sink)
		}
	}
}

func connect(strm *stream.Stream, self []byte, skey *rsa.PrivateKey, pkey *rsa.PublicKey) (ses *Session, err error) {
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
	return &Session{*strm, secret}, nil
}

func authenticate(strm *stream.Stream, key *rsa.PrivateKey, sink chan *Session) {
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
	err = session.Finalize(&key.PublicKey, resp.Token) // TODO: Key-store!!!
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
	sink <- &Session{*strm, secret}
}
