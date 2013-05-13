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

// This file contains the application  through which to access the carrier.

package carrier

import (
	"config"
	"fmt"
	"log"
	"math/big"
	"math/rand"
	"proto/overlay"
	"proto/session"
	"sync"
)

// A remote application address.
type Address struct {
	nodeId *big.Int
	appId  *big.Int
}

// A carrier adderss-tagged message.
type Message struct {
	Src *Address
	Msg *session.Message
}

// A carrier application through which to communicate.
type Application struct {
	id *big.Int // The id of the application
	c  *carrier // The carrier through which to communicate

	sink chan *Message            // Direct message delivery endpoint
	subs map[string]chan *Message // Active topic subscription delivery points

	lock sync.RWMutex
}

// Creates a new application through which to communicate with others.
func (c *carrier) NewApp() *Application {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Create the new application interface and store
	app := &Application{
		id:   big.NewInt(rand.Int63()),
		c:    c,
		sink: make(chan *Message, config.CarrierAppBuffer),
		subs: make(map[string]chan *Message),
	}
	c.apps[app.id.String()] = app

	return app
}

// Returns the application direct sink channel.
func (a *Application) Sink() chan *Message {
	return a.sink
}

// Subscribes to the specified topic and returns a new channel on which incoming
// messages will arrive.
func (a *Application) Subscribe(topic string) (chan *Message, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	key := overlay.Resolve(topic)

	// Create the subscription channel if non-existent
	if _, ok := a.subs[key.String()]; ok {
		return nil, fmt.Errorf("already subscribed")
	}
	sub := make(chan *Message, config.CarrierAppBuffer)
	a.subs[key.String()] = sub

	// Subscribe the app to the topic and initiate a subscription message
	a.c.handleSubscribe(a.id, key, true)
	a.c.sendSubscribe(key)

	return sub, nil
}

// Removes the subscription of ch from topic.
func (a *Application) Unsubscribe(topic string) {
	// Remove the carrier subscription
	key := overlay.Resolve(topic)
	a.c.handleUnsubscribe(a.id, key, true)

	// Clean up the application
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.subs, key.String())
}

// Publishes a message into topic to be broadcast to everyone.
func (a *Application) Publish(topic string, msg *session.Message) {
	a.c.sendPublish(a.id, overlay.Resolve(topic), msg)
}

// Delivers a message to a subscribed node, balancing amongst all subscriptions.
func (a *Application) Balance(topic string, msg *session.Message) {
	a.c.sendBalance(a.id, overlay.Resolve(topic), msg)
}

// Sends a direct message to a known app on a known node.
func (a *Application) Direct(dest *Address, msg *session.Message) {
	a.c.sendDirect(a.id, dest, msg)
}

// Delivers a topic subscription to the application.
func (a *Application) deliverSubscription(src *Address, topic *big.Int, msg *session.Message) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	if sink, ok := a.subs[topic.String()]; ok {
		select {
		case sink <- &Message{src, msg}:
			// Ok, we're happy
		default:
			log.Printf("application topic buffer overflow")
		}
	} else {
		log.Printf("unknown application subscription: %v.", topic)
	}
}

// Delivers a direct message to the application.
func (a *Application) deliverDirect(src *Address, msg *session.Message) {
	select {
	case a.sink <- &Message{src, msg}:
		// Ok, we're happy
	default:
		log.Printf("application buffer overflow")
	}
}
