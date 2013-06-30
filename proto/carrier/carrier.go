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
	"crypto/rsa"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/heart"
	"github.com/karalabe/iris/proto/carrier/topic"
	"github.com/karalabe/iris/proto/overlay"
	"sync"
	"time"
)

// The carrier interface to route all messages to desired topics.
type Carrier interface {
	Boot() error
	Shutdown()
	Connect(cb ConnectionCallback) *Connection
}

// The real carrier implementation, receiving the overlay events and processing
// them according to the protocol.
type carrier struct {
	transport *overlay.Overlay // Overlay network to route the messages
	heart     *heart.Heart     // Heartbeat mechanism

	topics map[string]*topic.Topic // Locally active topics
	conns  map[string]*Connection  // Locally active connections

	lock sync.RWMutex
}

// Creates a new messaging middleware carrier.
func New(overId string, key *rsa.PrivateKey) Carrier {
	// Create and initialize the overlay
	c := &carrier{
		topics: make(map[string]*topic.Topic),
		conns:  make(map[string]*Connection),
	}
	c.transport = overlay.New(overId, key, c)
	c.heart = heart.New(time.Duration(config.CarrierBeatPeriod)*time.Millisecond, config.CarrierKillCount, c)
	return c
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
