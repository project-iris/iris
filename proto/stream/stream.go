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

// Package stream wraps a TCP/IP network connection with the Go gob en/decoder.
//
// Note, in case of a serialization error (encoding or decoding failure), it is
// assumed that there is either a protocol mismatch between the parties, or an
// implementation bug; but in any case, the connection is deemed failed and is
// terminated.
package stream

import (
	"bufio"
	"encoding/gob"
	"log"
	"net"
	"time"
)

// TCP/IP based stream with a gob encoder on top.
type Stream struct {
	socket  *net.TCPConn      // Network connection to the remote endpoint
	buffers *bufio.ReadWriter // Buffered access to the network socket
	encoder *gob.Encoder      // Gob encoder for data serialization
	decoder *gob.Decoder      // Gob decoder for data deserialization
}

// Constants for the protocol TCP/IP layer
const acceptTimeout = time.Second

// Opens a TCP server socket, and if successful, starts a go routine for
// accepting incoming connections and returns a stream and a quit channel.
// If an auto-port (0) was requested, the port is updated in the argument.
func Listen(addr *net.TCPAddr) (chan *Stream, chan chan error, error) {
	// Open the server socket
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	addr.Port = sock.Addr().(*net.TCPAddr).Port

	// Create the two channels, start the acceptor and return
	sink := make(chan *Stream)
	quit := make(chan chan error)
	go accept(sock, sink, quit)
	return sink, quit, nil
}

// Accepts incoming connection requests, converts them info a TCP/IP gob stream
// and send them back on the sink channel.
func accept(sock *net.TCPListener, sink chan *Stream, quit chan chan error) {
	var errc chan error
	var errv error

	// Loop until an error occurs or quit is requested
	for errv == nil && errc == nil {
		select {
		case errc = <-quit:
			continue
		default:
			// Accept an incoming connection but without blocking for too long
			sock.SetDeadline(time.Now().Add(acceptTimeout))
			if conn, err := sock.AcceptTCP(); err == nil {
				sink <- newStream(conn)
			} else if !err.(net.Error).Timeout() {
				log.Printf("stream: failed to accept connection: %v.", err)
				errv = err
			}
		}
	}
	// Close the socket and upstream stream sink
	close(sink)
	if err := sock.Close(); err != nil && errv == nil {
		// Keep only first error
		errv = err
	}
	// Wait for termination sync and return
	if errc == nil {
		errc = <-quit
	}
	errc <- errv
}

// Creates a new, gob backed network stream based on a live TCP/IP connection.
func newStream(sock *net.TCPConn) *Stream {
	reader := bufio.NewReader(sock)
	writer := bufio.NewWriter(sock)

	return &Stream{
		socket:  sock,
		buffers: bufio.NewReadWriter(reader, writer),
		encoder: gob.NewEncoder(writer),
		decoder: gob.NewDecoder(reader),
	}
}

// Connects to a remote host and returns the connection stream.
func Dial(address string, timeout time.Duration) (*Stream, error) {
	if sock, err := net.DialTimeout("tcp", address, timeout); err != nil {
		return nil, err
	} else {
		return newStream(sock.(*net.TCPConn)), nil
	}
}

// Retrieves the raw connection object if special manipulations are needed.
func (s *Stream) Sock() *net.TCPConn {
	return s.socket
}

// Serializes an object and sends it over the wire. In case of an error, the
// connection is torn down.
func (s *Stream) Send(data interface{}) error {
	if err := s.encoder.Encode(data); err != nil {
		s.socket.Close()
		return err
	}
	return nil
}

// Flushes the outbound socket. In case of an error, the  network stream is torn
// down.
func (s *Stream) Flush() error {
	if err := s.buffers.Flush(); err != nil {
		s.socket.Close()
		return err
	}
	return nil
}

// Receives a gob of the given type and returns it. If an  error occurs, the
// network stream is torn down.
func (s *Stream) Recv(data interface{}) error {
	if err := s.decoder.Decode(data); err != nil {
		s.socket.Close()
		return err
	}
	return nil
}

// Closes the underlying network connection of a stream.
func (s *Stream) Close() error {
	return s.socket.Close()
}
