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

// Package gobber implements a buffer based datagram oriented gob coder.
package gobber

import (
	"bytes"
	"encoding/gob"
)

// A gob encoder and decoder for datagram messages.
type Gobber struct {
	outBuf bytes.Buffer
	inBuf  bytes.Buffer

	outEnc *gob.Encoder
	inDec  *gob.Decoder

	types []interface{}
}

// Creates and returns a new gobber.
func New() *Gobber {
	g := new(Gobber)
	g.outEnc = gob.NewEncoder(&g.outBuf)
	g.inDec = gob.NewDecoder(&g.inBuf)
	g.types = make([]interface{}, 0)
	return g
}

// Initializes the internal gob state to handle messages of the given type.
func (g *Gobber) Init(msg interface{}) error {
	// Save the message type for later reinits
	g.types = append(g.types, msg)

	// Build the encoder internal state
	if err := g.outEnc.Encode(msg); err != nil {
		return err
	}
	defer g.outBuf.Reset()

	// Build the decoder internal state
	g.inBuf.Write(g.outBuf.Bytes())
	if err := g.inDec.Decode(msg); err != nil {
		return err
	}
	return nil
}

// Encodes a message and returns a reference to the output buffer. The caller is
// responsible for copying the slice contents before the next call!
func (g *Gobber) Encode(msg interface{}) ([]byte, error) {
	// Encode the message, making sure not to leave junk inside
	defer g.outBuf.Reset()
	if err := g.outEnc.Encode(msg); err != nil {
		return nil, err
	}
	return g.outBuf.Bytes(), nil
}

// Decodes the source data assembling the requested message.
func (g *Gobber) Decode(data []byte, msg interface{}) error {
	// Decode the message, making sure not to leave junk inside
	defer g.inBuf.Reset()
	g.inBuf.Write(data)
	if err := g.inDec.Decode(msg); err != nil {
		// Internal state corrupt, create new coders
		g.outEnc = gob.NewEncoder(&g.outBuf)
		g.inDec = gob.NewDecoder(&g.inBuf)

		// Pass initializers through the get state back up
		oldTypes := append([]interface{}{}, g.types...)
		for _, msg := range oldTypes {
			if err := g.Init(msg); err != nil {
				panic(err)
			}
		}
		g.types = oldTypes

		// Return the original error
		return err
	}
	return nil
}
