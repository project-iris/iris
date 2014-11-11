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

// This file contains the identifier space definitions and operations for the
// overlay network: delta and distance calculation between two ids, and the
// common prefix + next digit extraction for the pastry routing tables.

package pastry

import (
	"io"
	"math/big"

	"github.com/project-iris/iris/config"
)

var modulo = new(big.Int).SetBit(new(big.Int), config.PastrySpace, 1)
var posmid = new(big.Int).Rsh(modulo, 1)
var negmid = new(big.Int).Mul(posmid, big.NewInt(-1))

// Special id slice implementing sort.Interface.
type idSlice struct {
	origin *big.Int
	data   []*big.Int
}

// Required for sort.Sort.
func (p idSlice) Len() int {
	return len(p.data)
}

// Required for sort.Sort.
func (p idSlice) Less(i, j int) bool {
	di := delta(p.origin, p.data[i])
	dj := delta(p.origin, p.data[j])
	return di.Cmp(dj) < 0
}

// Required for sort.Sort.
func (p idSlice) Swap(i, j int) {
	p.data[i], p.data[j] = p.data[j], p.data[i]
}

// Calculates the signed distance between two ids on the circular ID space
func delta(a, b *big.Int) *big.Int {
	d := new(big.Int).Sub(b, a)
	switch {
	case posmid.Cmp(d) < 0:
		d.Sub(d, modulo)
	case negmid.Cmp(d) > 0:
		d.Add(d, modulo)
	}
	return d
}

// Calculates the absolute distance between two ids on the circular ID space
func Distance(a, b *big.Int) *big.Int {
	return new(big.Int).Abs(delta(a, b))
}

// Calculate the length of the common prefix of two ids and the differing digit.
func prefix(a, b *big.Int) (int, int) {
	p := 0
	for bit := config.PastrySpace - 1; bit >= 0; bit-- {
		if a.Bit(bit) != b.Bit(bit) {
			p = (config.PastrySpace - 1 - bit) / config.PastryBase
			break
		}
	}
	d := uint(0)
	for bit := 0; bit < config.PastryBase; bit++ {
		d |= b.Bit(config.PastrySpace-(p+1)*config.PastryBase+bit) << uint(bit)
	}
	return p, int(d)
}

// Converts a string id into an overlay id.
func Resolve(id string) *big.Int {
	// Hash the textual id
	h := config.PastryResolver()
	io.WriteString(h, id)
	sum := h.Sum(nil)

	// Extract enough bits, and clear overflows
	raw := sum[:(config.PastrySpace+7)/8]
	for i := 0; i < len(raw)*8-config.PastrySpace; i++ {
		raw[0] &= ^byte(1 << (7 - uint(i)))
	}
	// Return the new id
	return new(big.Int).SetBytes(raw)
}
