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

package topic

import (
	"github.com/karalabe/iris/ext/sortext"
	"math/big"
	"testing"
)

func TestTopic(t *testing.T) {
	// Define some setup parameters for the test
	topicId := big.NewInt(314)

	nodeIds := []int64{1, 2, 3, 4, 5}
	nodes := make([]*big.Int, len(nodeIds))
	for i, id := range nodeIds {
		nodes[i] = big.NewInt(id)
	}
	sortext.BigInts(nodes)

	appIds := []int64{11, 12, 13, 14, 15}
	apps := make([]*big.Int, len(appIds))
	for i, id := range appIds {
		apps[i] = big.NewInt(id)
	}
	sortext.BigInts(apps)

	// Create the topic and check internal state
	top := New(topicId)
	if id := top.Self(); id.Cmp(topicId) != 0 {
		t.Fatalf("topic id mismatch: have %v, want %v.", id, topicId)
	}
	// Check subscribe and unsubscribe features
	for i, id := range nodes {
		top.SubscribeNode(id)
		if n := len(top.nodes); n != i+1 {
			t.Fatalf("topic child node count mismatch: have %v, want %v", n, i+1)
		}
	}
	for i, id := range nodes {
		top.UnsubscribeNode(id)
		if n := len(top.nodes); n != len(nodes)-1-i {
			t.Fatalf("topic child node count mismatch: have %v, want %v", n, len(nodes)-1-i)
		}
	}
	for i, id := range apps {
		top.SubscribeApp(id)
		if n := len(top.apps); n != i+1 {
			t.Fatalf("topic child app count mismatch: have %v, want %v", n, i+1)
		}
	}
	for i, id := range apps {
		top.UnsubscribeApp(id)
		if n := len(top.apps); n != len(apps)-1-i {
			t.Fatalf("topic child app count mismatch: have %v, want %v", n, len(apps)-1-i)
		}
	}
	// Subscribe everybody back
	for _, id := range nodes {
		top.SubscribeNode(id)
	}
	for _, id := range apps {
		top.SubscribeApp(id)
	}
	// Check broadcasting
	ns, as := top.Broadcast()
	if len(ns) != len(nodes) {
		t.Fatalf("broadcast node list length mismatch: have %v, want %v.", len(ns), len(nodes))
	}
	for i, id := range ns {
		if nodes[i].Cmp(id) != 0 {
			t.Fatalf("broadcast node %d mismatch: have %v, want %v.", i, id, nodes[i])
		}
	}
	if len(as) != len(apps) {
		t.Fatalf("broadcast app list length mismatch: have %v, want %v.", len(as), len(apps))
	}
	for i, id := range as {
		if apps[i].Cmp(id) != 0 {
			t.Fatalf("broadcast app %d mismatch: have %v, want %v.", i, id, apps[i])
		}
	}
	// Check load balancing (without one entry)
	ns, as = []*big.Int{}, []*big.Int{}
	for i := 0; i < 1000; i++ {
		n, a, err := top.Balance(nodes[0])
		if err != nil {
			t.Fatalf("failed to balance: %v.", err)
		}
		if (n == nil && a == nil) || (n != nil && a != nil) {
			t.Fatalf("invalid balancing result (both or none nil): node %v, app %v.", n, a)
		}
		if n != nil {
			ns = append(ns, n)
		} else {
			as = append(as, a)
		}
	}
	if len(ns) == 0 {
		t.Fatalf("no nodes have been balanced to")
	}
	if len(as) == 0 {
		t.Fatalf("no apps have been balanced to")
	}
	for i, id := range ns {
		if id.Cmp(nodes[0]) == 0 {
			t.Fatalf("balance %d: excluded node %v.", i, id)
		}
		idx := sortext.SearchBigInts(nodes, id)
		if idx >= len(nodes) || nodes[idx] != id {
			t.Fatalf("balance %d: invalid node id %v.", i, id)
		}
	}
	for i, id := range as {
		idx := sortext.SearchBigInts(apps, id)
		if idx >= len(apps) || apps[idx] != id {
			t.Fatalf("balance %d: invalid node id %v.", i, id)
		}
	}
	// Check load report generation
	ns, caps := top.GenerateReport()
	if len(ns) != len(nodes) || len(caps) != len(nodes) {
		t.Fatalf("report target size mismatch: have %v/%v nodes/caps, want %v.", len(ns), len(caps), len(nodes))
	}
	for i, cap := range caps {
		if cap != len(nodes) {
			t.Fatalf("capacity %d mismatch: have %v, want %v", i, cap, len(nodes))
		}
	}
	// Check load processing
	total := 1 // Local apps
	for i, id := range nodes {
		top.ProcessReport(id, 10*(i+1))
		total += 10 * (i + 1)
	}
	ns, caps = top.GenerateReport()
	for i, cap := range caps {
		if cap != total-10*(i+1) {
			t.Fatalf("capacity %d mismatch: have %v, want %v", i, cap, total-10*(i+1))
		}
	}
}
