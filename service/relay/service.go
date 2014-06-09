// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

package relay

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/project-iris/iris/proto/iris"
)

// Rate at which to check for relay termination.
var acceptPollRate = time.Second

// Relay service, listening on a local TCP port and accepting connections for
// joining the Iris network.
type Relay struct {
	address  *net.TCPAddr     // Listener address
	listener *net.TCPListener // Listener socket for the locally joining apps
	iris     *iris.Overlay    // Overlay through which connections are relayed

	clients map[*relay]struct{} // Active client connections

	done chan *relay     // Channel on which active clients signal termination
	quit chan chan error // Quit channel to synchronize relay termination
}

// Creates a new relay attached to a carrier and opens the listener socket on
// the specified local port.
func New(port int, overlay *iris.Overlay) (*Relay, error) {
	// Assemble the listener address
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return nil, err
	}
	// Return the relay service endpoint
	return &Relay{
		address: addr,
		iris:    overlay,
		clients: make(map[*relay]struct{}),
		done:    make(chan *relay),
		quit:    make(chan chan error),
	}, nil
}

// Starts accepting local relay connections.
func (r *Relay) Boot() error {
	// Open the server socket
	if sock, err := net.ListenTCP("tcp", r.address); err != nil {
		return err
	} else {
		r.listener = sock
	}
	// Start accepting connections
	go r.acceptor()
	return nil
}

// Closes all open connections and terminates the relaying service.
func (r *Relay) Terminate() error {
	errc := make(chan error, 1)
	r.quit <- errc
	return <-errc
}

// Accepts inbound connections till the service is terminated. For each one it
// starts a new handler and hands the socket over.
func (r *Relay) acceptor() {
	// Accept connections until termination request
	var errc chan error
	for errc == nil {
		select {
		case errc = <-r.quit:
			break
		case client := <-r.done:
			// A client terminated, remove from active list
			delete(r.clients, client)
			if err := client.report(); err != nil {
				log.Printf("relay: closing client error: %v.", err)
			}
		default:
			// Accept an incoming connection but without blocking for too long
			r.listener.SetDeadline(time.Now().Add(acceptPollRate))
			if sock, err := r.listener.Accept(); err == nil {
				if rel, err := r.acceptRelay(sock); err != nil {
					log.Printf("relay: accept failed: %v.", err)
				} else {
					r.clients[rel] = struct{}{}
				}
			} else if !err.(net.Error).Timeout() {
				log.Printf("relay: accept failed: %v, terminating.", err)
			}
		}
	}
	// In case of failure, wait for termination request
	if errc == nil {
		errc = <-r.quit
	}
	// Forcefully close all active client connections
	for rel, _ := range r.clients {
		rel.drop()
		<-r.done
	}
	for rel, _ := range r.clients {
		rel.report()
	}
	// Clean up and report
	errc <- r.listener.Close()
}
