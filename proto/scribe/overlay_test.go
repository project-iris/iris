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

package scribe

import (
	"crypto/x509"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/proto"
)

type collector struct {
	publish []*proto.Message
	balance []*proto.Message
	direct  []*proto.Message
	lock    sync.Mutex
}

func (c *collector) HandlePublish(sender *big.Int, topic string, msg *proto.Message) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.publish = append(c.publish, msg)
}

func (c *collector) HandleBalance(sender *big.Int, topic string, msg *proto.Message) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.balance = append(c.balance, msg)
}

func (c *collector) HandleDirect(sender *big.Int, msg *proto.Message) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.direct = append(c.direct, msg)
}

// Tests whether topic publishing work as expected.
func TestPublish(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	nodes := 10
	pubs := 100

	// Make sure there are enough ports to use
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()

	for i := 0; i < nodes; i++ {
		config.BootPorts = append(config.BootPorts, 65500+i)
	}
	// Load the private key and start a single scribe node
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Gradually start up scribe nodes, and execute one operation with each
	//   i % 2 == 0: subscriber
	//   i % 3 == 1: publisher
	//   otherwise:  simple forwarder
	coll := &collector{
		publish: []*proto.Message{},
		balance: []*proto.Message{},
		direct:  []*proto.Message{},
	}
	live := make([]*Overlay, 0, nodes)
	for i := 0; i < nodes; i++ {
		// Start the node
		node := New(overId, key, coll)
		live = append(live, node)

		if _, err := node.Boot(); err != nil {
			t.Fatalf("failed to boot scribe node: %v.", err)
		}
		time.Sleep(time.Second)

		// If it's a subscriber, subscribe
		if i%2 == 0 {
			if err := node.Subscribe(topicId); err != nil {
				t.Fatalf("failed to subscribe to topic: %v.", err)
			}
			time.Sleep(time.Second)
		}
		// If it's a publisher, send a message through
		if i%3 == 0 {
			for j := 0; j < pubs; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := node.Publish(topicId, msg); err != nil {
					t.Fatalf("failed to publish into topic: %v,", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.publish); n != pubs*(i/2+1) {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, pubs*(i/2+1))
			} else {
				// Reset the event collector
				coll.publish = coll.publish[:0]
			}
		}
	}
	// Execute the inverse of the previous sequence, gradually terminating the nodes
	for i := nodes - 1; i >= 0; i-- {
		// If it's a publisher, send a message through
		if i%3 == 0 {
			for j := 0; j < pubs; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := live[i].Publish(topicId, msg); err != nil {
					t.Fatalf("failed to publish into topic: %v,", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.publish); n != pubs*(i/2+1) {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, pubs*(i/2+1))
			} else {
				// Reset the event collector
				coll.publish = coll.publish[:0]
			}
		}
		// If it's a subscriber, unsubscribe
		if i%2 == 0 {
			if err := live[i].Unsubscribe(topicId); err != nil {
				t.Fatalf("failed to unsubscribe from topic: %v.", err)
			}
			time.Sleep(time.Second)
		}
		// Terminate the node
		if err := live[i].Shutdown(); err != nil {
			t.Fatalf("failed to terminate scribe node: %v.", err)
		}
		time.Sleep(time.Second)
	}
}

// Tests whether topic balancing work as expected.
func TestBalance(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	nodes := 10
	bals := 100

	// Make sure there are enough ports to use
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()

	for i := 0; i < nodes; i++ {
		config.BootPorts = append(config.BootPorts, 65500+i)
	}
	// Load the private key and start a single scribe node
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Gradually start up scribe nodes, and execute one operation with each
	//   i % 2 == 0: subscriber
	//   i % 3 == 1: balancer
	//   otherwise:  simple forwarder
	coll := &collector{
		publish: []*proto.Message{},
		balance: []*proto.Message{},
		direct:  []*proto.Message{},
	}
	live := make([]*Overlay, 0, nodes)
	for i := 0; i < nodes; i++ {
		// Start the node
		node := New(overId, key, coll)
		live = append(live, node)

		if _, err := node.Boot(); err != nil {
			t.Fatalf("failed to boot scribe node: %v.", err)
		}
		time.Sleep(time.Second)

		// If it's a subscriber, subscribe
		if i%2 == 0 {
			if err := node.Subscribe(topicId); err != nil {
				t.Fatalf("failed to subscribe to topic: %v.", err)
			}
			time.Sleep(time.Second)
		}
		// If it's a balancer, send a message through
		if i%3 == 0 {
			for j := 0; j < bals; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := node.Balance(topicId, msg); err != nil {
					t.Fatalf("failed to balance into topic: %v,", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.balance); n != bals {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, bals)
			} else {
				// Reset the event collector
				coll.balance = coll.balance[:0]
			}
		}
	}
	// Execute the inverse of the previous sequence, gradually terminating the nodes
	for i := nodes - 1; i >= 0; i-- {
		// If it's a balancer, send a message through
		if i%3 == 0 {
			for j := 0; j < bals; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := live[i].Balance(topicId, msg); err != nil {
					t.Fatalf("failed to balance into topic: %v,", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.balance); n != bals {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, bals)
			} else {
				// Reset the event collector
				coll.balance = coll.balance[:0]
			}
		}
		// If it's a subscriber, unsubscribe
		if i%2 == 0 {
			if err := live[i].Unsubscribe(topicId); err != nil {
				t.Fatalf("failed to unsubscribe from topic: %v.", err)
			}
			time.Sleep(time.Second)
		}
		// Terminate the node
		if err := live[i].Shutdown(); err != nil {
			t.Fatalf("failed to terminate scribe node: %v.", err)
		}
		time.Sleep(time.Second)
	}
}

// Tests whether direct addressing works.
func TestDirect(t *testing.T) {
	// Override the overlay configuration
	swapConfigs()
	defer swapConfigs()

	nodes := 9
	msgs := 100

	// Make sure there are enough ports to use
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()

	for i := 0; i < nodes; i++ {
		config.BootPorts = append(config.BootPorts, 65500+i)
	}
	// Load the private key and start a single scribe node
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Boot an origin node to receive the direct messages
	coll := &collector{
		publish: []*proto.Message{},
		balance: []*proto.Message{},
		direct:  []*proto.Message{},
	}
	origin := New(overId, key, coll)
	if _, err := origin.Boot(); err != nil {
		t.Fatalf("failed to boot origin node: %v.", err)
	}
	defer func() {
		if err := origin.Shutdown(); err != nil {
			t.Fatalf("failed to terminate origin node: %v.", err)
		}
	}()
	// Gradually start up scribe nodes, and send a direct message with every second
	live := make([]*Overlay, 0, nodes)
	for i := 0; i < nodes; i++ {
		// Start the node
		node := New(overId, key, coll)
		live = append(live, node)

		if _, err := node.Boot(); err != nil {
			t.Fatalf("failed to boot scribe node: %v.", err)
		}
		time.Sleep(time.Second)

		// If it's a messenger, send direct packets
		if i%2 == 0 {
			for j := 0; j < msgs; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := node.Direct(origin.pastry.Self(), msg); err != nil {
					t.Fatalf("failed to send direct message: %v.", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.direct); n != msgs {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, msgs)
			} else {
				// Reset the event collector
				coll.direct = coll.direct[:0]
			}
		}
	}
	// Execute the inverse of the previous sequence, gradually terminating the nodes
	for i := nodes - 1; i >= 0; i-- {
		// If it's a messenger, send direct packets
		if i%2 == 0 {
			for j := 0; j < msgs; j++ {
				msg := &proto.Message{
					Data: []byte{byte(i)},
				}
				if err := live[i].Direct(origin.pastry.Self(), msg); err != nil {
					t.Fatalf("failed to send direct message: %v.", err)
				}
			}
			// Wait a while and check event counts
			time.Sleep(time.Second)
			if n := len(coll.direct); n != msgs {
				t.Fatalf("arrive event mismatch: have %v, want %v", n, msgs)
			} else {
				// Reset the event collector
				coll.direct = coll.direct[:0]
			}
		}
		// Terminate the node
		if err := live[i].Shutdown(); err != nil {
			t.Fatalf("failed to terminate scribe node: %v.", err)
		}
		time.Sleep(time.Second)
	}
}
