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

package iris

import (
	"bytes"
	"crypto/x509"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
)

// Connection handler for the tunnel tests.
type tunneler struct {
	self   int    // Index of the owner node
	remote uint32 // Number of remote tunnel messages
}

func (r *tunneler) HandleBroadcast(msg []byte) {
	panic("Broadcast passed to tunnel handler")
}

func (r *tunneler) HandleRequest(req []byte, timeout time.Duration) ([]byte, error) {
	panic("Request passed to tunnel handler")
}

func (r *tunneler) HandleTunnel(tun *Tunnel) {
	for {
		if chunk, msg, err := tun.Recv(3 * time.Second); err == nil {
			if r.self != int(msg[0]) {
				atomic.AddUint32(&r.remote, 1)
			}
			if err := tun.Send(chunk, msg); err != nil {
				panic(err)
			}
		} else {
			break
		}
	}
	tun.Close()
}

func (r *tunneler) HandleDrop(reason error) {
	panic("Connection dropped on tunnel handler")
}

// Individual tunnel tests.
func TestTunnelSingleNodeSingleConn(t *testing.T) {
	testTunnel(t, 1, 1, 10, 10000)
}

func TestTunnelSingleNodeMultiConn(t *testing.T) {
	testTunnel(t, 1, 10, 10, 1000)
}

func TestTunnelMultiNodeSingleConn(t *testing.T) {
	testTunnel(t, 10, 1, 10, 1000)
}

func TestTunnelMultiNodeMultiConn(t *testing.T) {
	testTunnel(t, 5, 5, 5, 100) // ulimit exceeded if too large
}

// Tests multi node multi connection tunnel.
func testTunnel(t *testing.T, nodes, conns, tuns, msgs int) {
	// Configure the test
	swapConfigs()
	defer swapConfigs()

	olds := config.BootPorts
	for i := 0; i < nodes; i++ {
		config.BootPorts = append(config.BootPorts, 65000+i)
	}
	defer func() { config.BootPorts = olds }()

	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)
	overlay := "tunnel-test"
	cluster := fmt.Sprintf("tunnel-test-%d-%d", nodes, conns)

	// Boot the iris overlays
	liveNodes := make([]*Overlay, nodes)
	for i := 0; i < nodes; i++ {
		liveNodes[i] = New(overlay, key)
		if _, err := liveNodes[i].Boot(); err != nil {
			t.Fatalf("failed to boot iris overlay: %v.", err)
		}
		defer func(node *Overlay) {
			if err := node.Shutdown(); err != nil {
				t.Fatalf("failed to terminate iris node: %v.", err)
			}
		}(liveNodes[i])
	}
	// Connect to all nodes with a lot of clients
	liveHands := make(map[int][]*tunneler)
	liveConns := make(map[int][]*Connection)
	for i, node := range liveNodes {
		liveHands[i] = make([]*tunneler, conns)
		liveConns[i] = make([]*Connection, conns)
		for j := 0; j < conns; j++ {
			liveHands[i][j] = &tunneler{i, 0}
			conn, err := node.Connect(cluster, liveHands[i][j])
			if err != nil {
				t.Fatalf("failed to connect to the iris overlay: %v.", err)
			}
			liveConns[i][j] = conn

			defer func(conn *Connection) {
				if err := conn.Close(); err != nil {
					t.Fatalf("failed to close iris connection: %v.", err)
				}
			}(liveConns[i][j])
		}
	}
	// Make sure there is a little time to propagate state and reports (TODO, fix this)
	if nodes > 1 {
		time.Sleep(3 * time.Second)
	}
	// Request with each and every node in parallel
	pend := new(sync.WaitGroup)
	for i := 0; i < nodes; i++ {
		for j := 0; j < conns; j++ {
			for k := 0; k < tuns; k++ {
				pend.Add(1)
				go func(i, j, k int) {
					defer pend.Done()

					// Establish a new tunnel
					tun, err := liveConns[i][j].Tunnel(cluster, 3*time.Second)
					if err != nil {
						t.Fatalf("failed to establish new tunnel: %v.", err)
					}
					defer func() {
						if err := tun.Close(); err != nil {
							t.Fatalf("failed to tear down tunnel: %v.", err)
						}
					}()
					// Pass a load of messages through (sync)
					for l := 0; l < msgs; l++ {
						orig := []byte{byte(i), byte(j), byte(k), byte(l)}
						msg := make([]byte, len(orig))
						copy(msg, orig)

						if err := tun.Send(len(msg), msg); err != nil {
							t.Fatalf("failed to send message: %v.", err)
						}
						if chunk, msg, err := tun.Recv(3 * time.Second); err != nil {
							t.Fatalf("failed to receive message: %v.", err)
						} else if chunk != len(orig) {
							t.Fatalf("send/recv chunk mismatch: have %v, want %v.", chunk, len(orig))
						} else if bytes.Compare(orig, msg) != 0 {
							t.Fatalf("send/recv data mismatch: have %v, want %v.", msg, orig)
						}
					}
				}(i, j, k)
			}
		}
	}
	pend.Wait()

	// Log some warning if connections didn't get remote tunnels
	if nodes > 1 {
		for i := 0; i < nodes; i++ {
			for j := 0; j < conns; j++ {
				if liveHands[i][j].remote == 0 {
					t.Logf("%v/%v:%v/%v no remote tunnels received.", i, nodes, j, conns)
				}
			}
		}
	}
}
