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

// Package carrier contains the messaging middleware implementation. It is
// currently based on a simplified version of Scribe, where no topic based acls
// are defined.
package carrier

import (
	"config"
	"crypto/rsa"
	"heart"
	"math/big"
	"proto/carrier/topic"
	"proto/overlay"
	"proto/session"
	"sync"
	"time"
)

// The carrier interface to route all messages to desired topics.
type Carrier interface {
	Boot() error
	Shutdown()

	Subscribe(id *big.Int, topic string) chan *session.Message
	Unsubscribe(id *big.Int, topic string)
	Publish(id *big.Int, topic string, msg *session.Message)
	Balance(id *big.Int, topic string, msg *session.Message)
}

// The real carrier implementation, receiving the overlay events and processing
// them according to the protocol.
type carrier struct {
	transport *overlay.Overlay
	topics    map[string]*topic.Topic
	heart     *heart.Heart

	lock sync.RWMutex
}

// Creates a new messaging middleware carrier.
func New(overId string, key *rsa.PrivateKey) Carrier {
	// Create and initialize the overlay
	c := &carrier{
		topics: make(map[string]*topic.Topic),
	}
	c.transport = overlay.New(overId, key, c)
	c.heart = heart.New(time.Duration(config.CarrierBeatPeriod)*time.Millisecond, config.CarrierKillCount, c)
	return Carrier(c)
}

// Boots the message carrier.
func (c *carrier) Boot() error {
	if err := c.transport.Boot(); err != nil {
		return err
	}
	c.heart.Start()
	return nil
}

// Terminates the carrier and all lower layer network primitives.
func (c *carrier) Shutdown() {
	c.heart.Terminate()
	c.transport.Shutdown()
}

// Subscribes to the specified topic and returns a new channel on which incoming
// messages will arrive.
func (c *carrier) Subscribe(id *big.Int, topic string) chan *session.Message {
	key := overlay.Resolve(topic)

	// Subscribe the app to the topic and initiate a subscription message
	c.handleSubscribe(id, key, true)
	c.sendSubscribe(key)

	return nil
}

// Removes the subscription of ch from topic.
func (c *carrier) Unsubscribe(id *big.Int, topic string) {
	c.handleUnsubscribe(id, overlay.Resolve(topic), true)
}

// Publishes a message into topic to be broadcast to everyone.
func (c *carrier) Publish(id *big.Int, topic string, msg *session.Message) {
	c.sendPublish(id, overlay.Resolve(topic), msg)
}

// Delivers a message to a subscribed node, balancing amongst all subscriptions.
func (c *carrier) Balance(id *big.Int, topic string, msg *session.Message) {
	c.sendBalance(id, overlay.Resolve(topic), msg)
}
