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

package carrier

import (
	"github.com/karalabe/iris/proto"
	"github.com/karalabe/iris/proto/carrier/topic"
	"log"
	"math/big"
)

// Implements the overlay.Callback.Deliver method.
func (c *carrier) Deliver(msg *proto.Message, key *big.Int) {
	head := msg.Head.Meta.(*header)
	switch head.Op {
	case opSub:
		// Accept remote subscription if not self-passthrough or looping root discovery
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.handleSubscribe(head.SrcNode, key, false)
		}
	case opUnsub:
		// Accept remote unsubscription if not self-passthrough
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.handleUnsubscribe(head.SrcNode, head.Topic, false)
		}
	case opPub:
		// Topic root, broadcast must be handled
		if !c.handlePublish(msg, head.Topic, head.Prev) {
			// Simple race condition between unsubscribe and publish, left in for debug
			log.Printf("carrier: couldn't handle delivered publish event: %v.", msg)
		}
	case opBal:
		// Topic root, balance must be handled
		if !c.handleBalance(msg, head.Topic, head.Prev) {
			// Simple race condition between unsubscribe and balance, left in for debug
			log.Printf("carrier: couldn't handle delivered balance event: %v.", msg)
		}
	case opRep:
		// Load report, integrate only if we're the real destination
		if key.Cmp(c.transport.Self()) == 0 {
			c.handleReport(head.SrcNode, head.Report)
		} else {
			log.Printf("carrier: wrong delivery for load report: %v.", msg)
		}
	case opDir:
		// Direct message, deliver
		c.handleDirect(msg, head.DestApp)
	default:
		log.Printf("unknown opcode received: %v, %v", head.Op, head)
	}
}

// Implements the overlay.Callback.Forward method.
func (c *carrier) Forward(msg *proto.Message, key *big.Int) bool {
	head := msg.Head.Meta.(*header)

	// If subscription event, process locally and reinitiate
	if head.Op == opSub {
		// Accept remote subscription if not self-passthrough or looping root discovery
		if head.SrcNode.Cmp(c.transport.Self()) != 0 {
			c.handleSubscribe(head.SrcNode, key, false)
		}
		// Swap out the originating node to the current and forward
		head.SrcNode = c.transport.Self()
		return true
	}
	// Catch virgin publish messages and only blindly forward if cannot handle
	if head.Op == opPub && head.Prev == nil {
		return !c.handlePublish(msg, head.Topic, nil)
	}
	// Catch virgin balance messages and only blindly forward if cannot handle
	if head.Op == opBal && head.Prev == nil {
		return !c.handleBalance(msg, head.Topic, nil)
	}
	return true
}

// Handles the subscription event to a topic.
func (c *carrier) handleSubscribe(entityId, topicId *big.Int, app bool) {
	// Make sure the requested topic exists, then subscribe
	c.lock.Lock()
	sid := topicId.String()
	top, ok := c.topics[sid]
	if !ok {
		top = topic.New(topicId)
		c.topics[sid] = top
	}
	c.lock.Unlock()

	// Subscribe as a child (local or remote)
	if app {
		top.SubscribeApp(entityId)
	} else {
		if err := top.SubscribeNode(entityId); err != nil {
			log.Printf("carrier: failed to subscribe node %v to topic %v: %v.", entityId, topicId, err)
			return
		}
		if err := c.monitor(topicId, entityId); err != nil {
			log.Printf("carrier: failed to monitor node %v in topic %v: %v.", entityId, topicId, err)
		}
		// Respond immediately to the subscription with an empty report (fast parent discovery)
		rep := &report{
			Tops: []*big.Int{topicId},
			Caps: []int{1},
		}
		c.sendReport(entityId, rep)
	}
}

// Handles the unsubscription event from a topic.
func (c *carrier) handleUnsubscribe(entityId, topicId *big.Int, app bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Unsubscribe the entity from the topic
	sid := topicId.String()
	if top, ok := c.topics[sid]; ok {
		if app {
			top.UnsubscribeApp(entityId)
		} else {
			top.UnsubscribeNode(entityId)
			if err := c.unmonitor(topicId, entityId); err != nil {
				log.Printf("carrier: failed to unmonitor node %v in topic %v: %v.", entityId, topicId, err)
			}
		}
		// If topic became empty, send unsubscribe to parent, close and delete
		if top.Empty() {
			if parent := top.Parent(); parent != nil {
				if err := c.unmonitor(topicId, parent); err != nil {
					log.Printf("carrier: failed to unmonitor parent %v in topic %v: %v.", entityId, topicId, err)
				}
				top.Reown(nil)
				go c.sendUnsubscribe(parent, top.Self())
			}
			top.Close()
			delete(c.topics, sid)
		}
	}
}

// Handles the publish event of a topic.
func (c *carrier) handlePublish(msg *proto.Message, topicId *big.Int, prevHop *big.Int) bool {
	c.lock.RLock()
	top, ok := c.topics[topicId.String()]
	c.lock.RUnlock()

	// Fetch the topic to operate on
	if ok {
		// Extract the message headers
		head := msg.Head.Meta.(*header)

		// Send to all nodes in the topic (except the previous hop)
		nodes, apps := top.Broadcast()
		for _, id := range nodes {
			if prevHop == nil || id.Cmp(prevHop) != 0 {
				// Create a copy since overlay will modify headers
				cpy := new(proto.Message)
				*cpy = *msg
				cpy.Head.Meta = head.copy()

				c.fwdPublish(id, cpy)
			}
		}
		// Decrypt the network message
		plain := new(proto.Message)
		plain.Head = msg.Head
		plain.Head.Meta = msg.Head.Meta.(*header).Meta
		plain.Data = make([]byte, len(msg.Data))
		copy(plain.Data, msg.Data)
		if err := plain.Decrypt(); err != nil {
			// Log, discard and report processed
			log.Printf("carrier: failed to decrypt publish message: %v.\n", err)
			return true
		}
		// Deliver to all local apps
		for _, id := range apps {
			// Deliver to the application on the specific topic
			c.lock.RLock()
			app, ok := c.conns[id.String()]
			c.lock.RUnlock()

			if ok {
				app.deliverPublish(&Address{head.SrcNode, head.SrcApp}, topicId, plain)
			} else {
				// Simple race condition between unsubscribe and publish, left in for debug
				log.Printf("carrier: unknown application %v, discarding publish: %v.", app, plain)
			}
		}
		return true
	}
	return false
}

// Handles the load balancing event of a topic.
func (c *carrier) handleBalance(msg *proto.Message, topicId *big.Int, prevHop *big.Int) bool {
	c.lock.RLock()
	top, ok := c.topics[topicId.String()]
	c.lock.RUnlock()

	if ok {
		// Fetch the recipient and either forward or deliver
		node, app, err := top.Balance(prevHop)
		if err != nil {
			// Maybe the topic is terminating, log and return unhandled
			log.Printf("carrier: failed to handle balance request: %v.", err)
			return false
		}
		if node != nil {
			c.fwdBalance(node, msg)
		} else {
			c.lock.RLock()
			a, ok := c.conns[app.String()]
			c.lock.RUnlock()
			if ok {
				// Remove all carrier headers and decrypt
				head := msg.Head.Meta.(*header)
				msg.Head.Meta = head.Meta
				if err := msg.Decrypt(); err != nil {
					log.Printf("carrier: failed to decrypt balance message: %v.\n", err)
					return true
				}
				// Deliver to the application on the specific topic
				a.deliverPublish(&Address{head.SrcNode, head.SrcApp}, topicId, msg)
			} else {
				// Simple race condition between unsubscribe and publish, left in for debug
				log.Printf("carrier: unknown application %v, discarding balance: %v.", app, msg)
			}
		}
		return true
	}
	return false
}

// Handles a remote member report, possibly assigning a new parent to the topic.
func (c *carrier) handleReport(src *big.Int, rep *report) {
	// Update local topics with the remote reports
	for i, id := range rep.Tops {
		c.lock.RLock()
		top, ok := c.topics[id.String()]
		c.lock.RUnlock()

		if ok {
			// Insert the report into the topic and assign parent if needed
			if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
				if top.Parent() == nil {
					// Assign a new parent TODO: REWRITE!!!
					if err := c.monitor(id, src); err != nil {
						log.Printf("carrier: topic %v failed to monitor new parent %v: %v.", id, src, err)
					}
					top.Reown(src)

					// Insert the topic report now
					if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
						log.Printf("carrier: topic failed to process new parent report %v: %v.", rep.Caps[i], err)
					}
				} else {
					log.Printf("carrier: topic failed to process report %v: %v.", rep.Caps[i], err)
				}
			} else {
				// Report processed correctly, update the heart
				if err := c.ping(id, src); err != nil {
					log.Printf("carrier: failed to ping node %v in topic %v: %v.", src, id, err)
				}
			}
		} else {
			// Simple race condition between unsubscribe and report, left in for debug
			log.Printf("carrier: unknown topic %v, discarding load report %v.", id, rep.Caps[i])
		}
	}
}

// Handles the receiving of a direct message.
func (c *carrier) handleDirect(msg *proto.Message, appId *big.Int) {
	c.lock.RLock()
	app, ok := c.conns[appId.String()]
	c.lock.RUnlock()

	if ok {
		// Remove all carrier headers
		head := msg.Head.Meta.(*header)
		msg.Head.Meta = head.Meta
		if err := msg.Decrypt(); err != nil {
			log.Printf("carrier: failed to decrypt direct message: %v.\n", err)
			return
		}
		// Deliver the source-tagged message
		app.deliverDirect(&Address{head.SrcNode, head.SrcApp}, msg)
	} else {
		// Simple race condition between a direct send and close (e.g. tunnel ack and close), left if for debug
		log.Printf("carrier: unknown application %v, discarding direct: %v.", appId, msg)
	}
}
