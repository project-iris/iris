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

package iris

// Specialized error interface to allow querying timeout errors.
type Error interface {
	error
	Timeout() bool
}

// Relay error implemneting the Error interface.
type irisError struct {
	message string
	timeout bool
}

// Implements error.Error.
func (e *irisError) Error() string {
	return e.message
}

// Implements Error.Timeout.
func (e *irisError) Timeout() bool {
	return e.timeout
}

// Creates a timeout error.
func timeError(err error) error {
	return &irisError{
		message: err.Error(),
		timeout: true,
	}
}

// Creates a permanent error.
func permError(err error) error {
	return &irisError{
		message: err.Error(),
		timeout: false,
	}
}
