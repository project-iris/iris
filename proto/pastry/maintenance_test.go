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

package pastry

import (
	"crypto/x509"
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/ext/mathext"
)

func checkRoutes(t *testing.T, nodes []*Overlay) {
	// Synchronize overlay states
	for _, o := range nodes {
		o.lock.RLock()
		defer o.lock.RUnlock()
	}
	// Extract the ids from the running nodes
	ids := make([]*big.Int, len(nodes))
	for i, o := range nodes {
		ids[i] = o.nodeId
	}
	// Assemble the leafset of each node and verify
	for _, o := range nodes {
		sort.Sort(idSlice{o.nodeId, ids})
		origin := 0
		for o.nodeId.Cmp(ids[origin]) != 0 {
			origin++
		}
		min := mathext.MaxInt(0, origin-config.PastryLeaves/2)
		max := mathext.MinInt(len(ids), origin+config.PastryLeaves/2)
		leaves := ids[min:max]

		if len(leaves) != len(o.routes.leaves) {
			t.Fatalf("overlay %v: leafset mismatch: have %v, want %v.", o.nodeId, o.routes.leaves, leaves)
		} else {
			for i, leaf := range leaves {
				if leaf.Cmp(o.routes.leaves[i]) != 0 {
					t.Fatalf("overlay %v: leafset mismatch: have %v, want %v.", o.nodeId, o.routes.leaves, leaves)
					break
				}
			}
		}
	}
	// Check the routing table for each node
	for _, o := range nodes {
		for r, row := range o.routes.routes {
			for c, p := range row {
				if p == nil {
					// Check that indeed no id is valid for this entry
					for _, id := range ids {
						if id.Cmp(o.nodeId) != 0 {
							if pre, dig := prefix(o.nodeId, id); pre == r && dig == c {
								t.Fatalf("overlay %v: entry {%v, %v} missing: %v.", o.nodeId, r, c, id)
							}
						}
					}
				} else {
					// Check that the id is valid and indeed not some leftover
					if pre, dig := prefix(o.nodeId, p); pre != r || dig != c {
						t.Fatalf("overlay %v: entry {%v, %v} invalid: %v.", o.nodeId, r, c, p)
					}
					alive := false
					for _, id := range ids {
						if id.Cmp(p) == 0 {
							alive = true
							break
						}
					}
					if !alive {
						t.Fatalf("overlay %v: entry {%v, %v} already dead: %v.", o.nodeId, r, c, p)
					}
				}
			}
		}
	}
}

func TestMaintenance(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	originals := 3
	additions := 2

	// Make sure there are enough ports to use
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()

	for i := 0; i < originals+additions; i++ {
		config.BootPorts = append(config.BootPorts, 65520+i)
	}
	// Parse encryption key
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Start handful of nodes and ensure valid routing state
	nodes := []*Overlay{}
	for i := 0; i < originals; i++ {
		nodes = append(nodes, New(appId, key, new(nopCallback)))
		if _, err := nodes[i].Boot(); err != nil {
			t.Fatalf("failed to boot nodes: %v.", err)
		}
		defer nodes[i].Shutdown()
	}
	// Wait a while for state updates to propagate and check the routing table
	time.Sleep(100 * time.Millisecond)
	checkRoutes(t, nodes)

	// Start some additional nodes and ensure still valid routing state
	for i := 0; i < additions; i++ {
		nodes = append(nodes, New(appId, key, new(nopCallback)))
		if _, err := nodes[len(nodes)-1].Boot(); err != nil {
			t.Fatalf("failed to boot nodes: %v.", err)
		}
	}
	// Wait a while for state updates to propagate and check the routing table
	time.Sleep(100 * time.Millisecond)
	checkRoutes(t, nodes)

	// Terminate some nodes, and ensure still valid routing state
	for i := 0; i < additions; i++ {
		nodes[originals+i].Shutdown()
	}
	nodes = nodes[:originals]

	// Wait a while for state updates to propagate and check the routing table
	time.Sleep(100 * time.Millisecond)
	checkRoutes(t, nodes)
}

/*
func TestMaintenanceDOS(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	// Make sure there are enough ports to use (use a huge number to simplify test code)
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()
	for i := 0; i < 24; i++ {
		config.BootPorts = append(config.BootPorts, 40000+i)
	}
	// Parse encryption key
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Increment the overlays till the test fails
	for peers := 16; !t.Failed(); peers++ {
		log.Printf("Live go routines before starting %d peers: %d.", peers, runtime.NumGoroutine())

		// Start the batch of nodes
		nodes := []*Overlay{}
		boots := new(sync.WaitGroup)
		for i := 0; i < peers; i++ {
			nodes = append(nodes, New(appId, key, nil))
			boots.Add(1)
			go func(o *Overlay) {
				defer boots.Done()
				if _, err := o.Boot(); err != nil {
					t.Fatalf("failed to boot nodes: %v.", err)
				}
			}(nodes[i])
		}
		// Wait a while for the handshakes to complete
		done := time.After(10 * time.Second)
		for loop := true; loop; {
			select {
			case <-done:
				loop = false
			case <-time.After(500 * time.Millisecond):
				log.Printf("Live go routines: %d.", runtime.NumGoroutine())
			}
		}
		// Check the routing tables
		checkRoutes(t, nodes)

		// Make sure boots finished
		boots.Wait()

		// Terminate all nodes, irrelevant of their state
		log.Printf("Shutting down overlays...")
		for i := 0; i < peers; i++ {
			nodes[i].Shutdown()
		}
		log.Printf("Live go routines after cleanup: %d.", runtime.NumGoroutine())

		if runtime.NumGoroutine() > 5 {
			panic("leaked")
		}
	}
}
*/
