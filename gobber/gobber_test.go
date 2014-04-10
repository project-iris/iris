// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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

package gobber

import (
	"bytes"
	"testing"
)

type Msg struct {
	A, B, C int
	D, E, F string
	G, H, I []byte
}

var msgs = []Msg{
	{A: 1, D: "a", G: []byte{0x10}},
	{B: 2, E: "b", H: []byte{0x20}},
	{C: 3, F: "c", I: []byte{0x30}},
}

func equal(a, b *Msg) bool {
	return a.A == b.A && a.B == b.B && a.C == b.C &&
		a.D == b.D && a.E == b.E && a.F == b.F &&
		bytes.Compare(a.G, b.G) == 0 &&
		bytes.Compare(a.H, b.H) == 0 &&
		bytes.Compare(a.I, b.I) == 0
}

func TestGobber(t *testing.T) {
	// Create a gobber and initialize it with Msg
	gob := New()
	if err := gob.Init(&Msg{}); err != nil {
		t.Errorf("failed to initialize gobber: %v.", err)
	}
	// Pass through a bunch of messages and check output
	for i, msg := range msgs {
		if buffer, err := gob.Encode(msg); err != nil {
			t.Errorf("test %d: failed to encode message %v: %v.", i, msg, err)
		} else {
			var message Msg
			if err := gob.Decode(buffer, &message); err != nil {
				t.Errorf("test %d: failed to decode message %v: %v.", i, buffer, err)
			}
			if !equal(&message, &msg) {
				t.Errorf("test %d: message mismatch: have %v, want %v.", i, message, msg)
			}
		}
	}
}

func TestUninit(t *testing.T) {
	// Create an uninitialized gobber
	gob := New()

	// Check that a decode fails
	for rep := 0; rep < 2; rep++ {
		// Pass a message through the encoder to remove headers
		if _, err := gob.Encode(&Msg{}); err != nil {
			t.Errorf("failed to encode junk message: %v.", err)
		}
		if buffer, err := gob.Encode(&Msg{}); err != nil {
			t.Errorf("failed to encode test message: %v.", err)
		} else {
			var message Msg
			if err := gob.Decode(buffer, &message); err == nil {
				t.Errorf("succeeded to decode test message: %v.", buffer)
			}
		}
	}
}
