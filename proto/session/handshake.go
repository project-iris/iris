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

package session

import (
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/crypto/sts"
	"github.com/karalabe/iris/proto/stream"
)

// Authenticated connection request message. Contains the originators ID for
// key lookup and the client exponential.
type authRequest struct {
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

// Session listener to accept inbound authenticated sessions.
type Listener struct {
	Sink  chan *Session  // Channel receiving the accepted sessions
	pends sync.WaitGroup // Pending authentications to sync the sink closing

	socket *stream.Listener // Stream listener socket to accept connections on
	key    *rsa.PrivateKey  // Private RSA key to authenticate with
	quit   chan chan error  // Termination synchronization channel
}

// Starts a TCP listener to accept incoming sessions, returning the socket ready
// to accept. If an auto-port (0) is requested, the port is updated in the arg.
func Listen(addr *net.TCPAddr, key *rsa.PrivateKey) (*Listener, error) {
	// Open the stream listener socket
	sock, err := stream.Listen(addr)
	if err != nil {
		return nil, err
	}
	// Assemble and return the session listener
	return &Listener{
		Sink:   make(chan *Session),
		socket: sock,
		key:    key,
		quit:   make(chan chan error),
	}, nil
}

// Starts the session connection accepter, with a maximum timeout to wait for an
// established connection to be handled.
func (l *Listener) Accept(timeout time.Duration) {
	l.socket.Accept(config.SessionAcceptTimeout)
	go l.accepter(timeout)
}

// Terminates the acceptor and returns any encountered errors.
func (l *Listener) Close() error {
	errc := make(chan error)
	l.quit <- errc
	return <-errc
}

// Accepts incoming net connections and initiates an STS authentication for each of them. Those that
// successfully pass the protocol get sent back on the session channel.
func (l *Listener) accepter(timeout time.Duration) {
	var errc chan error
	var errv error

	// Loop until an error occurs or quit is requested
	for errv == nil && errc == nil {
		select {
		case errc = <-l.quit:
			continue
		case conn, ok := <-l.socket.Sink:
			// Negotiate an STS session if a connection was established
			if !ok {
				errv = errors.New("stream listener terminated")
			} else {
				l.pends.Add(1)
				go l.serverAuth(conn, timeout)
			}
		}
	}
	// Make sure all running auths either finish or time out and close upstream sink
	l.pends.Wait()
	close(l.Sink)

	// Close stream listener, keeping initial error, if any
	if err := l.socket.Close(); err != nil && errv == nil {
		errv = err
	}
	// Wait for termination sync and return
	if errc == nil {
		errc = <-l.quit
	}
	errc <- errv
}

// Connects to a remote node and negotiates a session.
func Dial(host string, port int, key *rsa.PrivateKey) (*Session, error) {
	// Open the stream connection
	addr := fmt.Sprintf("%s:%d", host, port)
	strm, err := stream.Dial(addr, config.SessionDialTimeout)
	if err != nil {
		return nil, err
	}
	// Set up the authenticated session
	secret, err := clientAuth(strm, key)
	if err != nil {
		log.Printf("session: failed to authenticate connection: %v.", err)
		if err := strm.Close(); err != nil {
			log.Printf("session: failed to close unauthenticated connection: %v.", err)
		}
	}
	return newSession(strm, secret, true), nil
}

// Client side of the STS session negotiation.
func clientAuth(strm *stream.Stream, key *rsa.PrivateKey) ([]byte, error) {
	// Set an overall time limit for the handshake to complete
	strm.Sock().SetDeadline(time.Now().Add(config.SessionShakeTimeout))
	defer strm.Sock().SetDeadline(time.Time{})

	// Create a new empty session
	stsSess, err := sts.New(rand.Reader, config.StsGroup, config.StsGenerator, config.StsCipher, config.StsCipherBits, config.StsSigHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create new session: %v", err)
	}
	// Initiate a key exchange, send the exponential
	exp, err := stsSess.Initiate()
	if err != nil {
		return nil, fmt.Errorf("failed to initiate key exchange: %v", err)
	}
	if err = strm.Send(authRequest{exp}); err != nil {
		return nil, fmt.Errorf("failed to send auth request: %v", err)
	}
	if err = strm.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush auth request: %v", err)
	}
	// Receive the foreign exponential and auth token and if verifies, send own auth
	chall := new(authChallenge)
	if err = strm.Recv(chall); err != nil {
		return nil, fmt.Errorf("failed to receive auth challenge: %v", err)
	}
	token, err := stsSess.Verify(rand.Reader, key, &key.PublicKey, chall.Exp, chall.Token)
	if err != nil {
		return nil, fmt.Errorf("failed to verify acceptor auth token: %v", err)
	}
	if err = strm.Send(authResponse{token}); err != nil {
		return nil, fmt.Errorf("failed to send auth response: %v", err)
	}
	if err = strm.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush auth response: %v", err)
	}
	return stsSess.Secret()
}

// Server side of the STS session negotiation.
func (l *Listener) serverAuth(strm *stream.Stream, timeout time.Duration) {
	// Make sure the authentication is synced with the sink
	defer l.pends.Done()

	// Set an overall time limit for the handshake to complete
	strm.Sock().SetDeadline(time.Now().Add(config.SessionShakeTimeout))
	defer strm.Sock().SetDeadline(time.Time{})

	// Authenticate and clean up if unsuccessful
	if secret, err := l.doServerAuth(strm); err != nil {
		log.Printf("session: failed to authenticate remote stream: %v.", err)
		if err = strm.Close(); err != nil {
			log.Printf("session: failed to close unauthenticated stream: %v.", err)
		}
	} else {
		sess := newSession(strm, secret, false)
		select {
		case l.Sink <- sess:
			// Ok
		case <-time.After(timeout):
			log.Printf("session: established session not handled in %v, dropping.", timeout)
			if err = strm.Close(); err != nil {
				log.Printf("session: failed to close established session: %v.", err)
			}
		}
	}
}

// Executes the server side authentication and returns wither the agreed secret
// session key or the a failure reason.
func (l *Listener) doServerAuth(strm *stream.Stream) ([]byte, error) {
	// Create a new STS session
	stsSess, err := sts.New(rand.Reader, config.StsGroup, config.StsGenerator,
		config.StsCipher, config.StsCipherBits, config.StsSigHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create STS session: %v", err)
	}
	// Receive foreign exponential, accept the incoming key exchange request and send back own exp + auth token
	req := new(authRequest)
	if err = strm.Recv(req); err != nil {
		return nil, fmt.Errorf("failed to retrieve auth request: %v", err)
	}
	exp, token, err := stsSess.Accept(rand.Reader, l.key, req.Exp)
	if err != nil {
		return nil, fmt.Errorf("failed to accept incoming exchange: %v", err)
	}
	if err = strm.Send(authChallenge{exp, token}); err != nil {
		return nil, fmt.Errorf("failed to encode auth challenge: %v", err)
	}
	if err = strm.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush auth challenge: %v", err)
	}
	// Receive the foreign auth token and if verifies conclude session
	resp := new(authResponse)
	if err = strm.Recv(resp); err != nil {
		return nil, fmt.Errorf("failed to decode auth response: %v", err)
	}
	if err = stsSess.Finalize(&l.key.PublicKey, resp.Token); err != nil {
		return nil, fmt.Errorf("failed to finalize exchange: %v", err)
	}
	return stsSess.Secret()
}
