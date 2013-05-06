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

package overlay

import (
	"config"
	"crypto/x509"
	"proto/session"
	"testing"
	"time"
)

type collector struct {
	delivs []*session.Message
}

func (c *collector) Deliver(msg *session.Message) {
	c.delivs = append(c.delivs, msg)
}

func TestRouting(t *testing.T) {
	// Make sure cleanups terminate before returning
	defer time.Sleep(3 * time.Second)

	// Make sure there are enough ports to use
	peers := 4
	olds := config.BootPorts
	defer func() { config.BootPorts = olds }()
	for i := 0; i < peers; i++ {
		config.BootPorts = append(config.BootPorts, 65520+i)
	}
	// Parse encryption key
	key, _ := x509.ParsePKCS1PrivateKey(privKeyDer)

	// Create the callbacks to listen on incoming messages
	apps := []*collector{}
	for i := 0; i < peers; i++ {
		apps = append(apps, &collector{[]*session.Message{}})
	}
	// Start handful of nodes and ensure valid routing state
	nodes := []*Overlay{}
	for i := 0; i < peers; i++ {
		nodes = append(nodes, New(appId, key, apps[i]))
		if err := nodes[i].Boot(); err != nil {
			t.Errorf("failed to boot nodes: %v.", err)
		}
		defer nodes[i].Shutdown()
	}
	// Wait a while for the handshakes to complete
	time.Sleep(3 * time.Second)

	// Check that each node can route to everybody
	for _, src := range nodes {
		for _, dst := range nodes {
			src.Send(dst.nodeId, new(session.Message))
		}
	}
	// Sleep a bit and verify
	time.Sleep(time.Second)
	for i := 0; i < peers; i++ {
		if len(apps[i].delivs) != peers {
			t.Errorf("app #%v: message count mismatch: %v.", i, apps[i].delivs)
		}
	}
}
