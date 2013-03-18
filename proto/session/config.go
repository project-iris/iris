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
package session

import (
	"crypto"
	"crypto/aes"
	"math/big"
)

// Cyclic group for the STS cryptography
var stsGroup = big.NewInt(3910779947)

// Cyclic group generator for the STS cryptography
var stsGenerator = big.NewInt(1213725007)

// Symmetric cipher to use for the STS encryption
var stsCipher = aes.NewCipher

// Key size for the symmetric cipher
var stsCipherBits = 128

// Hash type for the RSA signature/verification
var stsSigHash = crypto.MD5
