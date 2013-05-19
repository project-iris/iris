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
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/crypto/sts"
	"github.com/karalabe/iris/proto/stream"
	"log"
	"math/big"
	"net"
)

// Authenticated connection request message. Contains the originators ID for
// key lookup and the client exponential.
type authRequest struct {
	Id  string
	Exp *big.Int
}

// Authentication challenge message. Contains the server exponential and the
// server side auth token (both verification and challenge at the same time).
type authChallenge struct {
	Exp   *big.Int
	Token []byte
}

// Authentication challenge response message. Contains the client side token.
type authResponse struct {
	Token []byte
}

// Starts a listener on port to accept incoming sessions. The key/store pairs
// are used for the mutual authentication. On success, a session channel is
// returned which will receive the successfully authenticated clients; and a
// quit channel to be able to terminate the listener.
func Listen(addr *net.TCPAddr, key *rsa.PrivateKey, store map[string]*rsa.PublicKey) (chan *Session, chan struct{}, error) {
	// Open the TCP socket
	netSink, netQuit, err := stream.Listen(addr)
	if err != nil {
		return nil, nil, err
	}
	// For each incoming connection, execute auth negotiation
	sink := make(chan *Session)
	quit := make(chan struct{})
	go accept(key, store, sink, quit, netSink, netQuit)
	return sink, quit, nil
}

// Connects to a remote node and negotiates a session using the local secret key
// and the remote public key. On success, a new Session is returned to handle
// further communication.
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

// Client side of the STS session negotiation.
func connect(strm *stream.Stream, self string, skey *rsa.PrivateKey, pkey *rsa.PublicKey) (ses *Session, err error) {
	// Defer an error handler that will ensure a closed stream
	defer func() {
		if err != nil {
			strm.Close()
			ses = nil
		}
	}()
	// Create a new empty session
	session, err := sts.New(rand.Reader, config.StsGroup, config.StsGenerator,
		config.StsCipher, config.StsCipherBits, config.StsSigHash)
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
	return newSession(strm, secret, true), nil
}

// Server side of the STS session negotiation.
func authenticate(strm *stream.Stream, key *rsa.PrivateKey, store map[string]*rsa.PublicKey, sink chan *Session) {
	// Defer an error handler that will ensure a closed stream
	var err error
	defer func() {
		if err != nil {
			strm.Close()
		}
	}()
	// Create a new STS session
	session, err := sts.New(rand.Reader, config.StsGroup, config.StsGenerator,
		config.StsCipher, config.StsCipherBits, config.StsSigHash)
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
	_, ok := store[req.Id]
	if !ok {
		log.Printf("unknown connecting client: %v\n", req.Id)
		err = errors.New("unknown client")
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
	sink <- newSession(strm, secret, false)
}
