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

// Package session implements an encrypted data stream, authenticated through
// the station-to-station key exchange.
package session

import (
	"hash"

	"code.google.com/p/go.crypto/hkdf"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto/stream"
)

// Accomplishes secure and authenticated full duplex communication.
type Session struct {
	CtrlLink *Link // Network connection for high priority control messages
	DataLink *Link // Network connection for low priority data messages
}

// Creates a new, double link session for authenticated data transfer. The
// initiator is used to decide the key derivation order for the channels.
func newSession(strm *stream.Stream, secret []byte, initiator bool) *Session {
	// Create the key derivation function
	hasher := func() hash.Hash { return config.HkdfHash.New() }
	hkdf := hkdf.New(hasher, secret, config.HkdfSalt, config.HkdfInfo)

	// Create the encrypted control and (incomplete) data link
	ctrl := newLink(hkdf, initiator)
	data := newLink(hkdf, initiator)
	ctrl.socket = strm

	return &Session{
		CtrlLink: ctrl,
		DataLink: data,
	}
}

// Starts the session data transfers on the control and data channels.
func (s *Session) Start(cap int) {
	s.CtrlLink.start(cap)
	s.DataLink.start(cap)
}

// Terminates the data transfers on the two channels
func (s *Session) Close() error {
	res := s.CtrlLink.close()
	if err := s.DataLink.close(); res == nil {
		res = err
	}
	return res
}
