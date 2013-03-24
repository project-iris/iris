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

// Package stream wraps a TCP/IP network connection with the Go gob en/decoder.
//
// Note, in case of a serialization error (encoding or decoding failure), it is
// assumed that there is either a protocol mismatch between the parties, or an
// implementation bug; but in any case, the connection is deemed failed and is
// terminated.
package stream

import (
	"encoding/gob"
	"fmt"
	"net"
	"time"
)

// TCP/IP based stream with a gob encoder on top.
type Stream struct {
	sock net.Conn
	enc  *gob.Encoder
	dec  *gob.Decoder
}

// Constants for the protocol TCP/IP layer
var acceptTimeout = time.Second
var dialTimeout = time.Second

// Opens a tcp server socket, and if successful, starts a go routine for
// accepting incoming connections and returns a stream and a quit channel.
// If an auto-port (0) was requested, the port is returned in the addr arg.
func Listen(addr *net.TCPAddr) (chan *Stream, chan struct{}, error) {
	// Open the server socket
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	addr.Port = sock.Addr().(*net.TCPAddr).Port
	// Create the two channels, start the acceptor and return
	sink := make(chan *Stream)
	quit := make(chan struct{})
	go accept(sock, sink, quit)
	return sink, quit, nil
}

// Connects to a remote host and returns the connection stream.
func Dial(host string, port int) (*Stream, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	sock, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return newStream(sock), nil
}

// Accepts incoming connection requests, converts them info a TCP/IP gob stream
// and send them back on the sink channel.
func accept(sock *net.TCPListener, sink chan *Stream, quit chan struct{}) {
	defer close(sink)
	defer sock.Close()
	for {
		select {
		case <-quit:
			return
		default:
			// Accept an incoming connection but without blocking for too long
			sock.SetDeadline(time.Now().Add(acceptTimeout))
			conn, err := sock.Accept()
			if err == nil {
				sink <- newStream(conn)
			}
		}
	}
}

// Creates a new, gob backed network stream based on a live TCP/IP connection.
func newStream(sock net.Conn) *Stream {
	return &Stream{sock, gob.NewEncoder(sock), gob.NewDecoder(sock)}
}

// Serializes a data an sends it over the wire. In case of an error, the network
// stream is torn down.
func (s *Stream) Send(data interface{}) error {
	err := s.enc.Encode(data)
	if err != nil {
		s.sock.Close()
	}
	return err
}

// Receives a gob of the given type and returns it. If an  error occurs, the
// network stream is torn down.
func (s *Stream) Recv(data interface{}) error {
	err := s.dec.Decode(data)
	if err != nil {
		s.sock.Close()
	}
	return err
}

// Closes the underlying network connection of a stream.
func (s *Stream) Close() {
	s.sock.Close()
}
