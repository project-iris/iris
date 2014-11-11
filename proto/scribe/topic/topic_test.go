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

package topic

import (
	"math/big"
	"testing"

	"github.com/project-iris/iris/ext/sortext"
)

func TestTopic(t *testing.T) {
	// Define some setup parameters for the test
	topicId := big.NewInt(314)
	ownerId := big.NewInt(141)

	nodeIds := []int64{1, 2, 3, 4, 5}
	nodes := make([]*big.Int, len(nodeIds))
	for i, id := range nodeIds {
		nodes[i] = big.NewInt(id)
	}
	sortext.BigInts(nodes)

	// Create the topic and check internal state
	top := New(topicId, ownerId)
	if id := top.Self(); id.Cmp(topicId) != 0 {
		t.Fatalf("topic id mismatch: have %v, want %v.", id, topicId)
	}
	if id := top.owner; id.Cmp(ownerId) != 0 {
		t.Fatalf("topic owner mismatch: have %v, want %v.", id, ownerId)
	}
	// Check subscribe and unsubscribe
	for i, id := range nodes {
		top.Subscribe(id)
		if n := len(top.nodes); n != i+1 {
			t.Fatalf("topic child node count mismatch: have %v, want %v", n, i+1)
		}
	}
	for i, id := range nodes {
		top.Unsubscribe(id)
		if n := len(top.nodes); n != len(nodes)-1-i {
			t.Fatalf("topic child node count mismatch: have %v, want %v", n, len(nodes)-1-i)
		}
	}
	// Subscribe everybody back
	for _, id := range nodes {
		top.Subscribe(id)
	}
	// Check broadcasting with no exclusion
	ns := top.Broadcast(nil)
	if len(ns) != len(nodes) {
		t.Fatalf("broadcast node list length mismatch: have %v, want %v.", len(ns), len(nodes))
	}
	for i, id := range ns {
		if nodes[i].Cmp(id) != 0 {
			t.Fatalf("broadcast node %d mismatch: have %v, want %v.", i, id, nodes[i])
		}
	}
	// Check broadcasting with exclusion
	ns = top.Broadcast(nodes[0])
	if len(ns) != len(nodes)-1 {
		t.Fatalf("excluded broadcast node list length mismatch: have %v, want %v.", len(ns), len(nodes)-1)
	}
	for _, id := range ns {
		if nodes[0].Cmp(id) == 0 {
			t.Fatalf("broadcast includes excluded node: %v.", id)
		}
	}
	// Check load balancing (without one entry)
	ns = []*big.Int{}
	for i := 0; i < 1000; i++ {
		n, err := top.Balance(nodes[0])
		if err != nil {
			t.Fatalf("failed to balance: %v.", err)
		}
		ns = append(ns, n)
	}
	if len(ns) == 0 {
		t.Fatalf("no nodes have been balanced to")
	}
	for i, id := range ns {
		if id.Cmp(nodes[0]) == 0 {
			t.Fatalf("balance %d: reached excluded node %v.", i, id)
		}
		idx := sortext.SearchBigInts(nodes, id)
		if idx >= len(nodes) || nodes[idx] != id {
			t.Fatalf("balance %d: invalid node id %v.", i, id)
		}
	}
	// Add a local subscription
	if err := top.Subscribe(ownerId); err != nil {
		t.Fatalf("failed to subscribe with local node: %v.", err)
	}
	// Check load report generation
	ns, caps := top.GenerateReports()
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
	ns, caps = top.GenerateReports()
	for i, cap := range caps {
		if cap != total-10*(i+1) {
			t.Fatalf("capacity %d mismatch: have %v, want %v", i, cap, total-10*(i+1))
		}
	}
}
