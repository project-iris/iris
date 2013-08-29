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

// Contains some helper routines for the iris commands.

package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
)

// Tries to load an RSA private key from a file in either PEM or DER format,
// returning wither the parsed key or the error reason.
func parseRsaKey(path string) (*rsa.PrivateKey, error) {
	// Read the key contents
	rsaData, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Try processing as PEM format
	if block, _ := pem.Decode(rsaData); block != nil {
		return x509.ParsePKCS1PrivateKey(block.Bytes)
	}
	// Give it a shot as simple binary DER
	return x509.ParsePKCS1PrivateKey(rsaData)
}

// Similar to the log.Fatalf, only using fmt instead of log.
func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
