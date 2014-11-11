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

// Package scribe contains a simplified version of Scribe, where no topic based
// ACLs are defined.
package scribe

import (
	"crypto/rsa"
	"errors"
	"log"
	"math/big"
	"sync"

	"github.com/project-iris/iris/config"
	"github.com/project-iris/iris/heart"
	"github.com/project-iris/iris/proto"
	"github.com/project-iris/iris/proto/pastry"
	"github.com/project-iris/iris/proto/scribe/topic"
)

// Custom topic error messages
var ErrSubscribed = errors.New("already subscribed")

// Callback for events leaving the overlay network.
type Callback interface {
	HandlePublish(sender *big.Int, topic string, msg *proto.Message)
	HandleBalance(sender *big.Int, topic string, msg *proto.Message)
	HandleDirect(sender *big.Int, msg *proto.Message)
}

// The overlay implementation, receiving the overlay events and processing
// them according to the protocol.
type Overlay struct {
	app Callback // Upstream application callback

	pastry *pastry.Overlay // Overlay network to route the messages
	heart  *heart.Heart    // Heartbeat mechanism

	topics map[string]*topic.Topic // Topics active in the local node
	names  map[string]string       // Mapping from topic id to its textual name

	lock sync.RWMutex
}

// Creates a new scribe overlay.
func New(overId string, key *rsa.PrivateKey, app Callback) *Overlay {
	// Create and initialize the overlay
	o := &Overlay{
		app:    app,
		topics: make(map[string]*topic.Topic),
		names:  make(map[string]string),
	}
	o.pastry = pastry.New(overId, key, o)
	o.heart = heart.New(config.ScribeBeatPeriod, config.ScribeKillCount, o)
	return o
}

// Boots the overlay, returning the number of remote peers.
func (o *Overlay) Boot() (int, error) {
	log.Printf("scribe: booting with id %v.", o.pastry.Self())

	// Start the heartbeat first since convergence can last long
	o.heart.Start()

	// Boot the overlay and wait until it converges
	peers, err := o.pastry.Boot()
	if err != nil {
		return 0, err
	}
	return peers, nil
}

// Terminates the overlay and all lower layer network primitives.
func (o *Overlay) Shutdown() error {
	// Unsubscribe from all left-over topics
	o.lock.RLock()
	for id, topic := range o.names {
		log.Printf("scribe: removing left-over topic %v.", topic)
		sid, _ := new(big.Int).SetString(id, 10)
		go o.handleUnsubscribe(o.pastry.Self(), sid)
	}
	o.lock.RUnlock()

	// Terminate the heartbeat mechanism and shut down pastry
	o.heart.Terminate()
	return o.pastry.Shutdown()
}

// Subscribes to the specified scribe topic.
func (o *Overlay) Subscribe(topic string) error {
	// Resolve the topic id
	id := pastry.Resolve(topic)
	sid := id.String()

	// Make sure we can map the id back to the textual name
	o.lock.RLock()
	_, ok := o.names[sid]
	o.lock.RUnlock()
	if !ok {
		o.lock.Lock()
		o.names[sid] = topic
		o.lock.Unlock()
	}
	// Subscribe the local node to the topic
	return o.handleSubscribe(o.pastry.Self(), id)
}

// Removes the subscription from topic.
func (o *Overlay) Unsubscribe(topic string) error {
	// Resolve the topic id
	id := pastry.Resolve(topic)
	sid := id.String()

	// Remove the topic name mapping
	o.lock.Lock()
	delete(o.names, sid)
	o.lock.Unlock()

	// Remove the scribe subscription
	return o.handleUnsubscribe(o.pastry.Self(), id)
}

// Publishes a message into topic to be broadcast to everyone.
func (o *Overlay) Publish(topic string, msg *proto.Message) error {
	if err := msg.Encrypt(); err != nil {
		return err
	}
	o.sendPublish(pastry.Resolve(topic), msg)
	return nil
}

// Balances a message to one of the subscribed nodes.
func (o *Overlay) Balance(topic string, msg *proto.Message) error {
	if err := msg.Encrypt(); err != nil {
		return err
	}
	o.sendBalance(pastry.Resolve(topic), msg)
	return nil
}

// Sends a direct message to a known node.
func (o *Overlay) Direct(dest *big.Int, msg *proto.Message) error {
	if err := msg.Encrypt(); err != nil {
		return err
	}
	o.sendDirect(dest, msg)
	return nil
}
