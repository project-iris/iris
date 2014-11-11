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

// This file contains the different carrier event handlers.
//
// A short summary of how scribe handles different events:
//  - Subscription:
//    When a node wishes to subscribe to a topic, it will initiate a subscribe
//    message towards the topic rendez-vous point (node closest to the topic
//    hash). If such a subscription is being forwarded (pastry forward), it is
//    caught by the relaying node, and a subscription to it is made, after which
//    this middle node initiates a brand new subscription. If it is delivered,
//    hopefully the topic root was reached and subscription cascading stops.
//
//  - Subscription removal:
//    If all children nodes removed their subscription, and no local clients are
//    subscribed to a specific topic, the topic itself is removed and the parent
//    notified via an unsubscribe message. This message must be handled only by
//    the true parent (destination and recipient match exactly), since churn can
//    cause misplaced delivery.
//
//  - Publish:
//    Whenever an event is published into a topic, it is sent towards the groups
//    rendez-void node (the topic root in the multi-cast tree) for distribution.
//    However, if the message enters the tree midway, there's no point in routing
//    all the way to the root and back down, instead, distribution begins from
//    the entry point. To prevent message duplication, each publish is either in
//    a virgin or non-virgin state, depending on whether it entered the tree. If
//    an event is virgin, it can be caught by any member node and processed, but
//    non-virgin nodes must use precise addressing.
//
//  - Balance:
//    It is essentially the same as publish, with the only difference that the
//    message is send forward on only one edge of the multi-cast tree.
//
//  - Report:
//    These are used to distribute load reports between members of a multi-cast
//    tree. Since members know about each other, reports use precise addressing.
//
//  - Direct:
//    As the name suggests, direct messages have a precise destination. Only the
//    true recipient must handle it. Delivery to a non-precise destination means
//    either the destination terminated, or pastry's mis-delivered (churn?).

package scribe

import (
	"errors"
	"fmt"
	"log"
	"math/big"

	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/pastry"
	"github.com/project-iris/iris/proto/scribe/topic"
)

// Implements the pastry.Callback.Deliver method.
func (o *Overlay) Deliver(msg *proto.Message, key *big.Int) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opSubscribe:
		// Topic roots will get self-subscribe messages, discard them
		if head.Sender.Cmp(o.pastry.Self()) == 0 {
			return
		}
		if err := o.handleSubscribe(head.Sender, key); err != nil {
			log.Printf("scribe: %v failed to handle delivered subscription %v to %v: %v.", o.pastry.Self(), head.Sender, key, err)
		}
	case opUnsubscribe:
		// Drop all unsubscriptions not intended directly for the current node
		if o.pastry.Self().Cmp(key) != 0 {
			log.Printf("scribe: unsubscription delivered to wrong node (churn?): have %v, want %v.", key, o.pastry.Self())
			return
		}
		if err := o.handleUnsubscribe(head.Sender, head.Topic); err != nil {
			log.Printf("scribe: failed to handle delivered unsubscription: %v.", err)
		}
	case opPublish:
		// Non-virgin publishes must be delivered precisely
		if head.Prev != nil && o.pastry.Self().Cmp(key) != 0 {
			log.Printf("scribe: non-virgin publish at wrong destination (churn?): have %v, want %v.", key, o.pastry.Self())
			return
		}
		if hand, err := o.handlePublish(msg, head.Topic, head.Prev); !hand || err != nil {
			// Simple race condition between unsubscribe and publish, left in for debug
			log.Printf("scribe: %v failed to handle delivered publish (churn?): %v %v.", o.pastry.Self(), hand, err)
		}
	case opBalance:
		// Non-virgin balances must be delivered precisely
		if head.Prev != nil && o.pastry.Self().Cmp(key) != 0 {
			log.Printf("scribe: non-virgin balance at wrong destination (churn?): have %v, want %v.", key, o.pastry.Self())
			return
		}
		if hand, err := o.handleBalance(msg, head.Topic, head.Prev); !hand || err != nil {
			// Simple race condition between unsubscribe and balance, left in for debug
			log.Printf("scribe: failed to handle delivered balance: %v %v.", hand, err)
		}
	case opReport:
		// Load reports are always addresses precisely, drop any other
		if o.pastry.Self().Cmp(key) != 0 {
			log.Printf("scribe: load report delivered to wrong node (churn?): have %v, want %v.", key, o.pastry.Self())
			return
		}
		if err := o.handleReport(head.Sender, head.Report); err != nil {
			log.Printf("scribe: failed to handle remote load report: %v.", err)
		}
	case opDirect:
		// Direct messages are always precise
		if o.pastry.Self().Cmp(key) != 0 {
			log.Printf("scribe: direct message delivered to wrong node (churn?): have %v, want %v.", key, o.pastry.Self())
			return
		}
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
	if head.Op == opSubscribe {
		// Pastry always asks permission to forward, even local messages (bug? ugly maybe)
		// If the local node is the originator, just forward the message as is
		if head.Sender.Cmp(o.pastry.Self()) == 0 {
			return true
		}
		// Integrate the subscription locally
		if err := o.handleSubscribe(head.Sender, key); err != nil {
			// A failure most probably means double subscription caused by a race
			// between parent discovery and parent response. Discard to prevent the
			// node being registered into multiple subtrees.
			log.Printf("scribe: %v failed to handle forwarding subscription %v to %v: %v.", o.pastry.Self(), head.Sender, key, err)
			return false
		}
		// Integrated, cascade the subscription with the local node
		head.Sender = o.pastry.Self()
		return true
	}
	// Catch virgin publish messages and only blindly forward if cannot handle
	if head.Op == opPublish && head.Prev == nil {
		if hand, err := o.handlePublish(msg, head.Topic, head.Prev); err != nil {
			log.Printf("scribe: failed to handle forwarding publish: %v %v.", hand, err)
		} else {
			return !hand
		}
	}
	// Catch virgin balance messages and only blindly forward if cannot handle
	if head.Op == opBalance && head.Prev == nil {
		if hand, err := o.handleBalance(msg, head.Topic, head.Prev); err != nil {
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
	sid := topicId.String()

	// Fetch the topic or report not found
	o.lock.RLock()
	top, ok := o.topics[sid]
	topName := o.names[sid]
	o.lock.RUnlock()
	if !ok {
		// No error, but not handled either
		return false, nil
	}
	// Precise publish is accepted only from neighbors or self (subscription race)
	if prevHop != nil && !top.Neighbor(prevHop) {
		return true, fmt.Errorf("non-neighbor direct publish: %v", prevHop)
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
		o.app.HandlePublish(head.Sender, topName, plain)
	}
	return true, nil
}

// Handles the load balancing event of a topio.
func (o *Overlay) handleBalance(msg *proto.Message, topicId *big.Int, prevHop *big.Int) (bool, error) {
	sid := topicId.String()

	// Fetch the topic or report not found
	o.lock.RLock()
	top, ok := o.topics[sid]
	topName := o.names[sid]
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
	o.app.HandleBalance(head.Sender, topName, msg)
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
func (o *Overlay) handleReport(src *big.Int, rep *report) error {
	// Error collector
	errs := []error{}

	// Update local topics with the remote reports
	for i, id := range rep.Tops {
		o.lock.RLock()
		top, ok := o.topics[id.String()]
		o.lock.RUnlock()
		if !ok {
			// Simple race condition between unsubscribe and report, left in for debug
			errs = append(errs, fmt.Errorf("unknown topic: %v.", id))
			continue
		}
		// Insert the report into the topic and assign parent if needed
		if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
			// Report arrived from untracked node, assign as parent?
			if top.Parent() != nil {
				// Nope, we already have a parent, bin it
				errs = append(errs, fmt.Errorf("failed to process report: %v.", err))
				continue
			}
			// Make sure the node is closer than oneself. Prevents a race condition
			// between a child drop due to heart timeout and a late beat (report).
			if pastry.Distance(o.pastry.Self(), id).Cmp(pastry.Distance(src, id)) < 0 {
				errs = append(errs, fmt.Errorf("parent assignment denied: %v closer to %v than %v.", o.pastry.Self(), id, src))
				continue
			}
			// Assign a new parent node and reown
			if err := o.monitor(id, src); err != nil {
				errs = append(errs, fmt.Errorf("failed to monitor new parent: %v.", err))
				continue
			}
			top.Reown(src)

			// Insert the topic report now
			if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
				errs = append(errs, fmt.Errorf("failed to process parent report: %v.", err))
				continue
			}
		} else {
			// Report processed correctly, update the heart
			if err := o.ping(id, src); err != nil {
				errs = append(errs, fmt.Errorf("failed to ping node: %v.", err))
				continue
			}
		}
	}
	// Return any errors
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
