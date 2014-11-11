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
	endpoint  int                // Local port on which to listen on
	listeners []*net.TCPListener // Listener sockets for the locally joining apps

	iris    *iris.Overlay       // Overlay through which connections are relayed
	clients map[*relay]struct{} // Active client connections

	done chan *relay     // Channel on which active clients signal termination
	quit chan chan error // Quit channel to synchronize relay termination
}

// Creates a new relay attached to a carrier and opens the listener socket on
// the specified local port.
func New(port int, overlay *iris.Overlay) (*Relay, error) {
	return &Relay{
		endpoint:  port,
		listeners: []*net.TCPListener{},
		iris:      overlay,
		clients:   make(map[*relay]struct{}),
		done:      make(chan *relay),
		quit:      make(chan chan error),
	}, nil
}

// Starts accepting local relay connections.
func (r *Relay) Boot() error {
	errs := []error{}

	// Open the two (IPv4 and IPv6) listener sockets
	for _, addr := range []string{"127.0.0.1", "[::1]"} {
		if sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", addr, r.endpoint)); err != nil {
			log.Printf("relay: failed to listen on %s:%d: %v", addr, r.endpoint, err)
			errs = append(errs, err)
		} else {
			r.listeners = append(r.listeners, sock.(*net.TCPListener))
		}
	}
	if len(errs) == 2 {
		return fmt.Errorf("boot failed: %v (IPv4), %v (IPv6)", errs[0], errs[1])
	}
	// Start accepting connections
	for _, sock := range r.listeners {
		go r.acceptor(sock)
	}
	return nil
}

// Closes all open connections and terminates the relaying service.
func (r *Relay) Terminate() error {
	errs := []error{}
	errc := make(chan error, 1)

	for i := 0; i < len(r.listeners); i++ {
		r.quit <- errc
		if err := <-errc; err != nil {
			errs = append(errs, err)
		}
	}
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	case 2:
		return fmt.Errorf("%v", errs)
	}
	panic("unreachable code")
}

// Accepts inbound connections till the service is terminated. For each one it
// starts a new handler and hands the socket over.
func (r *Relay) acceptor(listener *net.TCPListener) {
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
			listener.SetDeadline(time.Now().Add(acceptPollRate))
			if sock, err := listener.Accept(); err == nil {
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
	errc <- listener.Close()
}
