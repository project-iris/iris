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

import "math/big"

// Authenticated connection request message. Contains the originators ID for
// key lookup and the client exponential.
type authRequest struct {
	Id  []byte
	Exp *big.Int
}

// Authentication challenge message. Contains the server exponential and the
// server side auth token (both verification and challenge at the same time).
type authChallenge struct {
	Exp   *big.Int
	Token []byte
}

// Authentication challenge response message. Contains the client side token.
type authResponse struct {
	Token []byte
}
