// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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

package link

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"code.google.com/p/go.crypto/hkdf"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/stream"
)

// Tests whether link ciphers are initializes correctly.
func TestCiphers(t *testing.T) {
	t.Parallel()

	// Generate a secret key for the HKDF
	secret := make([]byte, 16)
	io.ReadFull(rand.Reader, secret)

	// Create the server and client links (no connection between them)
	clientHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))
	serverHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))

	client := New(nil, clientHKDF, false)
	server := New(nil, serverHKDF, true)

	// Create some random data to operate on
	clientData := make([]byte, 4096)
	serverData := make([]byte, 4096)

	io.ReadFull(rand.Reader, clientData)
	copy(serverData, clientData)

	// Check that encryption and MACing match on the two sides
	for i := 0; i < 1000; i++ {
		client.inCipher.XORKeyStream(clientData, clientData)
		server.outCipher.XORKeyStream(serverData, serverData)
		if !bytes.Equal(clientData, serverData) {
			t.Fatalf("cipher mismatch on the session endpoints")
		}
		client.outCipher.XORKeyStream(clientData, clientData)
		server.inCipher.XORKeyStream(serverData, serverData)
		if !bytes.Equal(clientData, serverData) {
			t.Fatalf("cipher mismatch on the session endpoints")
		}
		client.inMacer.Write(clientData)
		server.outMacer.Write(serverData)
		clientData = client.inMacer.Sum(nil)
		serverData = server.outMacer.Sum(nil)
		if !bytes.Equal(clientData, serverData) {
			t.Fatalf("macer mismatch on the session endpoints")
		}
		client.outMacer.Write(clientData)
		server.inMacer.Write(serverData)
		clientData = client.outMacer.Sum(nil)
		serverData = server.inMacer.Sum(nil)
		if !bytes.Equal(clientData, serverData) {
			t.Fatalf("macer mismatch on the session endpoints")
		}
	}
}

// Tests the low level send and receive methods.
func TestDirectSendRecv(t *testing.T) {
	t.Parallel()

	// Start a stream listener
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve local address: %v.", err)
	}
	listener, err := stream.Listen(addr)
	if err != nil {
		t.Fatalf("failed to listen for incoming streams: %v.", err)
	}
	listener.Accept(10 * time.Millisecond)
	defer listener.Close()

	// Establish a stream connection to the listener
	host := fmt.Sprintf("%s:%d", "localhost", addr.Port)
	clientStrm, err := stream.Dial(host, time.Millisecond)
	if err != nil {
		t.Fatalf("failed to connect to stream listener: %v.", err)
	}
	serverStrm := <-listener.Sink

	defer clientStrm.Close()
	defer serverStrm.Close()

	// Initialize the stream based encrypted links
	secret := make([]byte, 16)
	io.ReadFull(rand.Reader, secret)

	clientHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))
	serverHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))

	clientLink := New(clientStrm, clientHKDF, false)
	serverLink := New(serverStrm, serverHKDF, true)

	// Generate some random messages and pass around both ways
	for i := 0; i < 1000; i++ {
		// Generate the message to send
		send := &proto.Message{
			Head: proto.Header{
				Meta: make([]byte, 32),
			},
			Data: make([]byte, 32),
		}
		io.ReadFull(rand.Reader, send.Head.Meta.([]byte))
		io.ReadFull(rand.Reader, send.Data)
		send.Encrypt()

		// Send the message from client to server
		if err := clientLink.SendDirect(send); err != nil {
			t.Fatalf("failed to send message to server: %v.", err)
		}
		if recv, err := serverLink.RecvDirect(); err != nil {
			t.Fatalf("failed to receive message from client: %v.", err)
		} else if bytes.Compare(send.Head.Meta.([]byte), recv.Head.Meta.([]byte)) != 0 || bytes.Compare(send.Data, recv.Data) != 0 {
			t.Fatalf("send/receive mismatch: have %+v, want %+v.", recv, send)
		}
		// Send the message from server to client
		if err := serverLink.SendDirect(send); err != nil {
			t.Fatalf("failed to send message to client: %v.", err)
		}
		if recv, err := clientLink.RecvDirect(); err != nil {
			t.Fatalf("failed to receive message from server: %v.", err)
		} else if bytes.Compare(send.Head.Meta.([]byte), recv.Head.Meta.([]byte)) != 0 || bytes.Compare(send.Data, recv.Data) != 0 {
			t.Fatalf("send/receive mismatch: have %+v, want %+v.", recv, send)
		}
	}
}

// Tests the high level send and receive mechanisms.
func TestSendRecv(t *testing.T) {
	t.Parallel()

	// Start a stream listener
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to resolve local address: %v.", err)
	}
	listener, err := stream.Listen(addr)
	if err != nil {
		t.Fatalf("failed to listen for incoming streams: %v.", err)
	}
	listener.Accept(10 * time.Millisecond)
	defer listener.Close()

	// Establish a stream connection to the listener
	host := fmt.Sprintf("%s:%d", "localhost", addr.Port)
	clientStrm, err := stream.Dial(host, time.Millisecond)
	if err != nil {
		t.Fatalf("failed to connect to stream listener: %v.", err)
	}
	serverStrm := <-listener.Sink

	// Initialize the stream based encrypted links
	secret := make([]byte, 16)
	io.ReadFull(rand.Reader, secret)

	clientHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))
	serverHKDF := hkdf.New(sha1.New, secret, []byte("HKDF salt"), []byte("HKDF info"))

	clientLink := New(clientStrm, clientHKDF, false)
	serverLink := New(serverStrm, serverHKDF, true)

	clientLink.Start(32)
	serverLink.Start(32)

	// Generate some random messages and pass around both ways
	for i := 0; i < 1000; i++ {
		// Generate the message to send
		send := &proto.Message{
			Head: proto.Header{
				Meta: make([]byte, 32),
			},
			Data: make([]byte, 32),
		}
		io.ReadFull(rand.Reader, send.Head.Meta.([]byte))
		io.ReadFull(rand.Reader, send.Data)
		send.Encrypt()

		// Send the message from client to server
		select {
		case clientLink.Send <- send:
			// Ok
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("client send timed out")
		}
		select {
		case recv, ok := <-serverLink.Recv:
			if !ok {
				t.Fatalf("server link closed prematurely")
			}
			if bytes.Compare(send.Head.Meta.([]byte), recv.Head.Meta.([]byte)) != 0 || bytes.Compare(send.Data, recv.Data) != 0 {
				t.Fatalf("send/receive mismatch: have %+v, want %+v.", recv, send)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("server receive timed out")
		}
		// Send the message from server to client
		select {
		case serverLink.Send <- send:
			// Ok
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("server send timed out")
		}
		select {
		case recv, ok := <-clientLink.Recv:
			if !ok {
				t.Fatalf("client link closed prematurely")
			}
			if bytes.Compare(send.Head.Meta.([]byte), recv.Head.Meta.([]byte)) != 0 || bytes.Compare(send.Data, recv.Data) != 0 {
				t.Fatalf("send/receive mismatch: have %+v, want %+v.", recv, send)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("client receive timed out")
		}
	}
	// Ensure the links can be successfully torn down
	go func() {
		if err := clientLink.Close(); err != nil {
			t.Fatalf("failed to close client link: %v.", err)
		}
	}()
	if err := serverLink.Close(); err != nil {
		t.Fatalf("failed to close server link: %v.", err)
	}
}
