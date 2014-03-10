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

// This file contains the different carrier event handlers.

package scribe

import (
	"errors"
	"log"
	"math/big"

	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/scribe/topic"
)

// Implements the pastry.Callback.Deliver method.
func (o *Overlay) Deliver(msg *proto.Message, key *big.Int) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opSub:
		// Accept remote subscription if not self-pass-through or looping root discovery
		if head.Sender.Cmp(o.pastry.Self()) != 0 {
			if err := o.handleSubscribe(head.Sender, key); err != nil {
				log.Printf("scribe: failed to handle delivered subscription: %v.", err)
			}
		}
	case opUnsub:
		// Accept remote unsubscription if not self-pass-through
		if head.Sender.Cmp(o.pastry.Self()) != 0 {
			if err := o.handleUnsubscribe(head.Sender, head.Topic); err != nil {
				log.Printf("scribe: failed to handle delivered unsubscription: %v.", err)
			}
		}
	case opPub:
		// Topic root, broadcast must be handled
		if hand, err := o.handlePublish(msg, head.Topic, head.Prev); !hand || err != nil {
			// Simple race condition between unsubscribe and publish, left in for debug
			log.Printf("scribe: failed to handle delivered publish: %v %v.", hand, err)
		}
	case opBal:
		// Topic root, balance must be handled
		if hand, err := o.handleBalance(msg, head.Topic, head.Prev); !hand || err != nil {
			// Simple race condition between unsubscribe and balance, left in for debug
			log.Printf("scribe: failed to handle delivered balance: %v %v.", hand, err)
		}
	case opRep:
		// Load report, integrate only if we're the real destination
		if key.Cmp(o.pastry.Self()) == 0 {
			o.handleReport(head.Sender, head.Report)
		} else {
			log.Printf("scribe: wrong load report destination: have %v, want %v.", key, o.pastry.Self())
		}
	case opDir:
		// Direct message, deliver
		if err := o.handleDirect(msg); err != nil {
			log.Printf("scribe: failed to handle direct message: %v.", err)
		}
	default:
		log.Printf("unknown opcode received: %v, %v", head.Op, head)
	}
}

// Implements the pastry.Callback.Forward method.
func (o *Overlay) Forward(msg *proto.Message, key *big.Int) bool {
	head := msg.Head.Meta.(*header)

	// If subscription event, process locally and re-initiate
	if head.Op == opSub {
		// Accept remote subscription if not self-pass-through or looping root discovery
		if head.Sender.Cmp(o.pastry.Self()) != 0 {
			if err := o.handleSubscribe(head.Sender, key); err != nil {
				log.Printf("scribe: failed to handle forwarding subscription: %v.", err)
				return true
			}
		}
		// Swap out the originating node to the current and forward
		head.Sender = o.pastry.Self()
		return true
	}
	// Catch virgin publish messages and only blindly forward if cannot handle
	if head.Op == opPub && head.Prev == nil {
		if hand, err := o.handlePublish(msg, head.Topic, nil); err != nil {
			log.Printf("scribe: failed to handle forwarding publish: %v %v.", hand, err)
		} else {
			return !hand
		}
	}
	// Catch virgin balance messages and only blindly forward if cannot handle
	if head.Op == opBal && head.Prev == nil {
		if hand, err := o.handleBalance(msg, head.Topic, nil); err != nil {
			log.Printf("scribe: failed to handle forwarding balance: %v %v.", hand, err)
		} else {
			return !hand
		}
	}
	return true
}

// Handles the subscription event to a topic.
func (o *Overlay) handleSubscribe(nodeId, topicId *big.Int) error {
	// Generate the textual topic id
	sid := topicId.String()

	// Make sure the requested topic exists, then subscribe
	o.lock.Lock()
	top, ok := o.topics[sid]
	if !ok {
		top = topic.New(topicId, o.pastry.Self())
		o.topics[sid] = top
	}
	o.lock.Unlock()

	// Subscribe node to the topic
	if err := top.Subscribe(nodeId); err != nil {
		return err
	}
	// If a remote node, start monitoring is and respond with an empty report (fast parent discovery)
	if nodeId.Cmp(o.pastry.Self()) != 0 {
		if err := o.monitor(topicId, nodeId); err != nil {
			return err
		}
		rep := &report{
			Tops: []*big.Int{topicId},
			Caps: []int{1},
		}
		o.sendReport(nodeId, rep)
	}
	return nil
}

// Handles the unsubscription event from a topic.
func (o *Overlay) handleUnsubscribe(nodeId, topicId *big.Int) error {
	o.lock.Lock()
	defer o.lock.Unlock()

	// Fetch the topic and ensure it exists
	sid := topicId.String()
	top, ok := o.topics[sid]
	if !ok {
		return errors.New("non-existent topic")
	}
	// Unsubscribe node from the topic and unmonitor if remote
	if err := top.Unsubscribe(nodeId); err != nil {
		return err
	}
	if nodeId.Cmp(o.pastry.Self()) != 0 {
		if err := o.unmonitor(topicId, nodeId); err != nil {
			return err
		}
	}
	// If topic became empty, send unsubscribe to parent and delete
	if top.Empty() {
		if parent := top.Parent(); parent != nil {
			if err := o.unmonitor(topicId, parent); err != nil {
				return err
			}
			top.Reown(nil)
			go o.sendUnsubscribe(parent, top.Self())
		}
		delete(o.topics, sid)
	}
	return nil
}

// Handles the publish event of a topic.
func (o *Overlay) handlePublish(msg *proto.Message, topicId *big.Int, prevHop *big.Int) (bool, error) {
	// Fetch the topic or report not found
	o.lock.RLock()
	top, ok := o.topics[topicId.String()]
	o.lock.RUnlock()
	if !ok {
		// No error, but not handled either
		return false, nil
	}
	// Extract the message headers
	head := msg.Head.Meta.(*header)

	// Get the batch of nodes to broadcast to
	nodes, local := top.Broadcast(prevHop), false
	owner := o.pastry.Self()
	for _, id := range nodes {
		if id.Cmp(owner) != 0 {
			// Create a copy since overlay will modify headers
			cpy := new(proto.Message)
			*cpy = *msg
			cpy.Head.Meta = head.copy()

			o.fwdPublish(id, cpy)
		} else {
			local = true
		}
	}
	// If local subscription is present, decrypt and deliver
	if local {
		// Assemble a fresh copy for decryption
		plain := &proto.Message{
			Head: msg.Head,
			Data: make([]byte, len(msg.Data)),
		}
		plain.Head.Meta = msg.Head.Meta.(*header).Meta
		copy(plain.Data, msg.Data)

		// Decrypt the message and deliver upstream
		if err := plain.Decrypt(); err != nil {
			// Cannot decrypt, report handled and also the error
			return true, err
		}
		o.app.HandlePublish(head.Sender, o.names[topicId.String()], plain)
	}
	return true, nil
}

// Handles the load balancing event of a topio.
func (o *Overlay) handleBalance(msg *proto.Message, topicId *big.Int, prevHop *big.Int) (bool, error) {
	// Fetch the topic or report not found
	o.lock.RLock()
	top, ok := o.topics[topicId.String()]
	o.lock.RUnlock()
	if !ok {
		// No error, but not handled either
		return false, nil
	}
	// Fetch the recipient and either forward or deliver
	node, err := top.Balance(prevHop)
	if err != nil {
		return true, err
	}
	// If it's a remote node, forward
	if node.Cmp(o.pastry.Self()) != 0 {
		o.fwdBalance(node, msg)
		return true, nil
	}
	// Remove all carrier headers and decrypt
	head := msg.Head.Meta.(*header)
	msg.Head.Meta = head.Meta
	if err := msg.Decrypt(); err != nil {
		return true, err
	}
	// Deliver to the application on the specific topic
	o.app.HandleBalance(head.Sender, o.names[topicId.String()], msg)
	return true, nil
}

// Handles the receiving of a direct message and delivers the contents upstream.
func (o *Overlay) handleDirect(msg *proto.Message) error {
	// Remove all scribe headers and decrypt contents
	head := msg.Head.Meta.(*header)
	msg.Head.Meta = head.Meta
	if err := msg.Decrypt(); err != nil {
		return err
	}
	// Deliver the message upstream
	o.app.HandleDirect(head.Sender, msg)
	return nil
}

// Handles a remote member report, possibly assigning a new parent to the topic.
func (o *Overlay) handleReport(src *big.Int, rep *report) {
	// Update local topics with the remote reports
	for i, id := range rep.Tops {
		o.lock.RLock()
		top, ok := o.topics[id.String()]
		o.lock.RUnlock()
		if !ok {
			// Simple race condition between unsubscribe and report, left in for debug
			log.Printf("scribe: unknown topic %v, discarding load report %v.", id, rep.Caps[i])
			continue
		}
		// Insert the report into the topic and assign parent if needed
		if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
			if top.Parent() != nil {
				log.Printf("scribe: topic failed to process report %v: %v.", rep.Caps[i], err)
				continue
			}
			// Assign a new parent TODO: REWRITE!!!
			if err := o.monitor(id, src); err != nil {
				log.Printf("scribe: topic %v failed to monitor new parent %v: %v.", id, src, err)
				continue
			}
			top.Reown(src)

			// Insert the topic report now
			if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
				log.Printf("scribe: topic failed to process new parent report %v: %v.", rep.Caps[i], err)
				continue
			}
		} else {
			// Report processed correctly, update the heart
			if err := o.ping(id, src); err != nil {
				log.Printf("scribe: failed to ping node %v in topic %v: %v.", src, id, err)
				continue
			}
		}
	}
}
