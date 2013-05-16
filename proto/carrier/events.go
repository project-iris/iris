// Iris - Distributed Messaging Framework
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
	"config"
	"log"
	"math/big"
	"proto/carrier/topic"
	"proto/session"
	"time"
)

// Implements the overlay.Callback.Deliver method.
func (c *carrier) Deliver(msg *session.Message, key *big.Int) {
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
			log.Printf("couldn't handle delivered publish event: %v.", msg)
		}
	case opBal:
		// Topic root, balance must be handled
		if !c.handleBalance(msg, head.Topic, head.Prev) {
			log.Printf("couldn't handle delivered balance event: %v.", msg)
		}
	case opRep:
		// Load report, integrate into topics
		c.handleReport(head.SrcNode, head.Report)
	case opDir:
		// Direct message, deliver
		c.handleDirect(msg, head.DestApp)
	default:
		log.Printf("unknown opcode received: %v, %v", head.Op, head)
	}
}

// Implements the overlay.Callback.Forward method.
func (c *carrier) Forward(msg *session.Message, key *big.Int) bool {
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
	c.lock.Lock()
	defer c.lock.Unlock()

	// Make sure the requested topic exists, then subscribe
	sid := topicId.String()
	top, ok := c.topics[sid]
	if !ok {
		beat := time.Duration(config.CarrierBeatPeriod) * time.Millisecond
		kill := config.CarrierKillCount

		top = topic.New(topicId, beat, kill)
		c.topics[sid] = top
	}
	// Subscribe as a child (local or remote)
	if app {
		top.SubscribeApp(entityId)
	} else {
		top.SubscribeNode(entityId)

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
		}
		// If topic became empty, send unsubscribe to parent and delete
		if top.Empty() {
			if parent := top.Parent(); parent != nil {
				c.sendUnsubscribe(parent, top.Self())
			}
			delete(c.topics, sid)
		}
	}
}

// Handles the publish event of a topic.
func (c *carrier) handlePublish(msg *session.Message, topicId *big.Int, prevHop *big.Int) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Fetch the topic to operate on
	sid := topicId.String()
	if top, ok := c.topics[sid]; ok {
		// Send to all nodes in the topic (except the previous hop)
		nodes, apps := top.Broadcast()
		for _, id := range nodes {
			if prevHop == nil || id.Cmp(prevHop) != 0 {
				// Create a copy since overlay will modify headers
				cpy := new(session.Message)
				*cpy = *msg
				cpy.Head.Meta = msg.Head.Meta.(*header).copy()

				c.fwdPublish(id, cpy)
			}
		}
		// Deliver to all local apps
		head := msg.Head.Meta.(*header)
		for _, id := range apps {
			// Copy and remove all carrier messages
			cpy := new(session.Message)
			*cpy = *msg
			cpy.Head.Meta = head.Meta

			// Deliver to the application on the specific topic
			if app, ok := c.conns[id.String()]; ok {
				app.deliverPublish(&Address{head.SrcNode, head.SrcApp}, topicId, cpy)
			} else {
				log.Printf("unknown application %v, discarding message.", app)
			}
		}
		return true
	}
	return false
}

// Handles the load balancing event of a topic.
func (c *carrier) handleBalance(msg *session.Message, topicId *big.Int, prevHop *big.Int) bool {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if top, ok := c.topics[topicId.String()]; ok {
		// Fetch the recipient and either forward or deliver
		node, app := top.Balance(prevHop)
		if node != nil {
			c.fwdBalance(node, msg)
		} else {
			if a, ok := c.conns[app.String()]; ok {
				// Remove all carrier headers
				head := msg.Head.Meta.(*header)
				msg.Head.Meta = head.Meta

				// Deliver to the application on the specific topic
				a.deliverPublish(&Address{head.SrcNode, head.SrcApp}, topicId, msg)
			} else {
				log.Printf("unknown application %v, discarding message.", app)
			}
		}
		return true
	}
	return false
}

// Handles a remote member report, possibly assigning a new parent to the topic.
func (c *carrier) handleReport(src *big.Int, rep *report) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// Update local topics with the remote reports
	for i, id := range rep.Tops {
		if top, ok := c.topics[id.String()]; ok {
			// Insert the report into the topic and assign parent if needed
			if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
				if top.Parent() == nil {
					top.Reown(src)
					if err := top.ProcessReport(src, rep.Caps[i]); err != nil {
						log.Printf("topic failed to process new parent report: %v.", err)
					}
				} else {
					log.Printf("topic failed to process report: %v.", err)
				}
			}
		} else {
			log.Printf("unknown topic %v, discarding load report %v.", id, rep.Caps[i])
		}
	}
}

// Handles the receiving of a direct message.
func (c *carrier) handleDirect(msg *session.Message, appId *big.Int) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if app, ok := c.conns[appId.String()]; ok {
		// Remove all carrier headers
		head := msg.Head.Meta.(*header)
		msg.Head.Meta = head.Meta

		// Deliver the source-tagged message
		app.deliverDirect(&Address{head.SrcNode, head.SrcApp}, msg)
	} else {
		log.Printf("unknown application %v, discarding direct message %v.", appId, msg)
	}
}
