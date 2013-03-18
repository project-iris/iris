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
	"net"
	"fmt"
	"time"
)

var acceptTimeout = time.Second
var dialTimeout   = time.Second

// Opens a tcp server socket, and if successful, starts a go routine for
// accepting incoming connections and returns a control and a sock channel.
func Listen(port int) (chan int, chan net.Conn, error) {
	// Open the server socket
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, nil, err
	}
  sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	// Create the management channel, start the new thread and return
	ops  := make(chan int)
	sink := make(chan net.Conn)
	go accept(sock, ops, sink)
	return ops, sink, nil
}

// Connects to a remote host on the given port and returns the connection.
func Dial(host string, port int) (net.Conn, error) {
	addr      := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Accepts incoming connection requests and starts a session negotiation
// on a new go routine.
func accept(sock *net.TCPListener, ops chan int, sink chan net.Conn) {
	defer sock.Close()
	for {
    select {
		case <- ops:
			// Process any control messages (exit for the moment)
			return
		default:
			// Accept an incoming connection but without blocking for too long
			sock.SetDeadline(time.Now().Add(acceptTimeout))
			conn, err := sock.Accept()
			if err == nil {
				sink <- conn
			}
		}
	}
}

