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

package session

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/big"
	rng "math/rand"
	"net"
	"sync"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/crypto/sts"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/stream"
)

// Session handshake request multiplexer to choose between the authenticated
// control channel handshake or the secondary data channel handshake.
type initRequest struct {
	Auth *authRequest
	Link *linkRequest
}

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

// Data channel linking request message. Used both to init, reply and verify.
type linkRequest struct {
	Id int64
}

// Make sure the link request packet is registered with gob.
func init() {
	gob.Register(&linkRequest{})
}

// Session listener to accept inbound authenticated sessions.
type Listener struct {
	Sink chan *Session // Channel receiving the accepted sessions

	pends    map[int64]chan *stream.Stream // Channels for finalizing the data connection linking
	pendLock sync.RWMutex                  // Lock to protect the pending map
	pendWait sync.WaitGroup                // Counter to prevent closing the session sink prematurely

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
		pends:  make(map[int64]chan *stream.Stream),
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

// Accepts incoming net connections and initiates either an STS authentication
// if primary control channel is being established, or a link negotiation it
// secondary data channel attachment request.
func (l *Listener) accepter(timeout time.Duration) {
	var errc chan error
	var errv error

	// Loop until an error occurs or quit is requested
	for errv == nil && errc == nil {
		select {
		case errc = <-l.quit:
			continue
		case conn, ok := <-l.socket.Sink:
			if !ok {
				errv = errors.New("stream listener terminated")
			} else {
				l.pendWait.Add(1)
				go l.serverHandle(conn, timeout)
			}
		}
	}
	// Close stream listener, keeping initial error, if any
	if err := l.socket.Close(); errv == nil {
		errv = err
	}
	// Make sure all running auths either finish or time out and close upstream sink
	l.pendWait.Wait()
	close(l.Sink)

	// Wait for termination sync and return
	if errc == nil {
		errc = <-l.quit
	}
	errc <- errv
}

// Server side of the STS session negotiation.
func (l *Listener) serverHandle(strm *stream.Stream, timeout time.Duration) {
	// Make sure the authentication is synced with the sink
	defer l.pendWait.Done()

	// Set an overall time limit for the handshake to complete
	strm.Sock().SetDeadline(time.Now().Add(config.SessionShakeTimeout))
	defer strm.Sock().SetDeadline(time.Time{})

	// Fetch the session request and multiplex on the contents
	req := new(initRequest)
	if err := strm.Recv(req); err != nil {
		log.Printf("session: failed to retrieve initiation request: %v", err)
		if err = strm.Close(); err != nil {
			log.Printf("session: failed to close uninitialized stream: %v.", err)
		}
		return
	}
	switch {
	case req.Auth != nil:
		// Authenticate and clean up if unsuccessful
		secret, err := l.serverAuth(strm, req.Auth)
		if err != nil {
			log.Printf("session: failed to authenticate remote stream: %v.", err)
			if err = strm.Close(); err != nil {
				log.Printf("session: failed to close unauthenticated stream: %v.", err)
			}
			return
		}
		// Create the session and link a data channel to it
		sess := newSession(strm, secret, true)
		if err = l.serverLink(sess); err != nil {
			log.Printf("session: failed to retrieve data link: %v.", err)
			if err = strm.Close(); err != nil {
				log.Printf("session: failed to close unlinked stream: %v.", err)
			}
			return
		}
		// Session setup complete, send upstream
		select {
		case l.Sink <- sess:
			// Ok
		case <-time.After(timeout):
			log.Printf("session: established session not handled in %v, dropping.", timeout)
			if err = sess.Close(); err != nil {
				log.Printf("session: failed to close established session: %v.", err)
			}
		}
	case req.Link != nil:
		// Extract the temporary session id and link this stream to it
		l.pendLock.Lock()
		res, ok := l.pends[req.Link.Id]
		l.pendLock.Unlock()

		if ok {
			select {
			case res <- strm:
				// Ok, link succeeded
			default:
				log.Printf("session: established data stream not handled.")
				if err := strm.Close(); err != nil {
					log.Printf("session: failed to close established data stream: %v.", err)
				}
			}
		}
	}
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
	// Link a new data connection to it
	sess := newSession(strm, secret, false)
	if err = clientLink(sess); err != nil {
		log.Printf("session: failed to link data connection: %v.", err)
		if err := strm.Close(); err != nil {
			log.Printf("session: failed to close unlinked connection: %v.", err)
		}
		return nil, err
	}
	return sess, nil
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
	req := &initRequest{
		Auth: &authRequest{exp},
	}
	if err = strm.Send(req); err != nil {
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

// Executes the server side authentication and returns either the agreed secret
// session key or the a failure reason.
func (l *Listener) serverAuth(strm *stream.Stream, req *authRequest) ([]byte, error) {
	// Create a new STS session
	stsSess, err := sts.New(rand.Reader, config.StsGroup, config.StsGenerator,
		config.StsCipher, config.StsCipherBits, config.StsSigHash)
	if err != nil {
		return nil, fmt.Errorf("failed to create STS session: %v", err)
	}
	// Accept the incoming key exchange request and send back own exp + auth token
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

// Initializes a data channel linking process, waiting for the data stream to be
// assigned.
func (l *Listener) serverLink(sess *Session) error {
	var err error

	// Create the a temporary channel to retrieve the data stream
	id, data := rng.Int63(), make(chan *stream.Stream)
	l.pendLock.Lock()
	l.pends[id] = data
	l.pendLock.Unlock()

	// Result independently clean up temporary id and channel
	defer func() {
		l.pendLock.Lock()
		delete(l.pends, id)
		l.pendLock.Unlock()
	}()
	// Send over the temporary session id to the client for data link setup
	msg := &proto.Message{
		Head: proto.Header{
			Meta: &linkRequest{id},
		},
	}
	if err = sess.CtrlLink.SendDirect(msg); err != nil {
		return fmt.Errorf("failed to send session id: %v", err)
	}
	// Wait for the data link or time out
	select {
	case strm := <-data:
		sess.init(strm, true)
	case <-time.After(config.SessionLinkTimeout):
		return errors.New("link timeout")
	}
	// Send the data link authentication
	auth := &proto.Message{
		Head: proto.Header{
			Meta: &linkRequest{id},
		},
	}
	// Retrieve the remote data link authentication
	if err = sess.DataLink.SendDirect(auth); err != nil {
		return fmt.Errorf("failed to send data auth: %v", err)
	}
	if msg, err := sess.DataLink.RecvDirect(); err != nil {
		return fmt.Errorf("failed to retrieve data auth: %v", err)
	} else if res, ok := msg.Head.Meta.(*linkRequest); !ok {
		return errors.New("corrupt auth message")
	} else if res.Id != id {
		return errors.New("mismatched auth message")
	}
	return nil
}

// Initiates a data channel link to the specified control channel.
func clientLink(sess *Session) error {
	// Wait for the server to specify the session id
	msg, err := sess.CtrlLink.RecvDirect()
	if err != nil {
		return fmt.Errorf("failed to retrieve session id: %v", err)
	}
	// Initiate a new stream connection to the server
	addr := sess.CtrlLink.Sock().RemoteAddr().String()
	strm, err := stream.Dial(addr, config.SessionDialTimeout)
	if err != nil {
		return fmt.Errorf("failed to establish data link: %v", err)
	}
	// Send the temporary id back on the data stream
	req := &initRequest{
		Link: &linkRequest{msg.Head.Meta.(*linkRequest).Id},
	}
	if err = strm.Send(req); err != nil {
		strm.Close()
		return fmt.Errorf("failed to send link request: %v", err)
	}
	if err = strm.Flush(); err != nil {
		strm.Close()
		return fmt.Errorf("failed to flush link request: %v", err)
	}
	// Finalize the session with the data stream
	sess.init(strm, false)

	// Send the data link authentication
	auth := &proto.Message{
		Head: proto.Header{
			Meta: req.Link,
		},
	}
	// Retrieve the remote data link authentication
	if err = sess.DataLink.SendDirect(auth); err != nil {
		return fmt.Errorf("failed to send data auth: %v", err)
	}
	if msg, err := sess.DataLink.RecvDirect(); err != nil {
		return fmt.Errorf("failed to retrieve data auth: %v", err)
	} else if res, ok := msg.Head.Meta.(*linkRequest); !ok {
		return errors.New("corrupt authentication message")
	} else if res.Id != req.Link.Id {
		return errors.New("mismatched authentication message")
	}
	return nil
}
