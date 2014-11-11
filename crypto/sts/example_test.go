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

package sts_test

import (
	"bytes"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	_ "crypto/sha1"
	"fmt"
	"math/big"

	"github.com/project-iris/iris/crypto/sts"
)

// Full STS communication example illustrated with two concurrent Go routines agreeing on a master key.
func Example_usage() {
	// STS cyclic group parameters, global for the app (small examples, not secure!)
	group := big.NewInt(3910779947)
	generator := big.NewInt(1213725007)

	// STS encryption parameters
	cipher := aes.NewCipher
	bits := 128
	hash := crypto.SHA1

	// RSA key-pairs for the communicating parties, obtained from somewhere else (no error checks)
	iniKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	accKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	// Start two Go routines: one initiator and one acceptor communicating on a channel
	transport := make(chan []byte)
	iniOut := make(chan []byte)
	accOut := make(chan []byte)

	go initiator(group, generator, cipher, bits, hash, iniKey, &accKey.PublicKey, transport, iniOut)
	go acceptor(group, generator, cipher, bits, hash, accKey, &iniKey.PublicKey, transport, accOut)

	// Check that the parties agreed upon the same master key
	iniMaster, iniOk := <-iniOut
	accMaster, accOk := <-accOut

	fmt.Printf("Initiator key valid: %v\n", iniOk && iniMaster != nil)
	fmt.Printf("Acceptor key valid: %v\n", accOk && accMaster != nil)
	fmt.Printf("Keys match: %v\n", bytes.Equal(iniMaster, accMaster))

	// Output:
	// Initiator key valid: true
	// Acceptor key valid: true
	// Keys match: true
}

// STS initiator: creates a new session, initiates a key exchange, verifies the other side and authenticates itself.
func initiator(group, generator *big.Int, cipher func([]byte) (cipher.Block, error), bits int,
	hash crypto.Hash, skey *rsa.PrivateKey, pkey *rsa.PublicKey, trans, out chan []byte) {
	// Create a new empty session
	session, err := sts.New(rand.Reader, group, generator, cipher, bits, hash)
	if err != nil {
		fmt.Printf("failed to create new session: %v\n", err)
		close(out)
		return
	}
	// Initiate a key exchange, send the exponential
	exp, err := session.Initiate()
	if err != nil {
		fmt.Printf("failed to initiate key exchange: %v\n", err)
		close(out)
		return
	}
	trans <- exp.Bytes()

	// Receive the foreign exponential and auth token and if verifies, send own auth
	bytes, token := <-trans, <-trans
	exp = new(big.Int).SetBytes(bytes)
	token, err = session.Verify(rand.Reader, skey, pkey, exp, token)
	if err != nil {
		fmt.Printf("failed to verify acceptor auth token: %v\n", err)
		close(out)
		return
	}
	trans <- token

	// Protocol done, other side should finalize if all is correct
	secret, err := session.Secret()
	if err != nil {
		fmt.Printf("failed to retrieve exchanged secret: %v\n", err)
		close(out)
		return
	}
	out <- secret
	return
}

// STS acceptor: creates a new session, accepts an exchange request, authenticates itself and verifies the other side.
func acceptor(group, generator *big.Int, cipher func([]byte) (cipher.Block, error), bits int,
	hash crypto.Hash, skey *rsa.PrivateKey, pkey *rsa.PublicKey, trans, out chan []byte) {
	// Create a new empty session
	session, err := sts.New(rand.Reader, group, generator, cipher, bits, hash)
	if err != nil {
		fmt.Printf("failed to create new session: %v\n", err)
		close(out)
		return
	}
	// Receive foreign exponential, accept the incoming key exchange request and send back own exp + auth token
	bytes := <-trans
	exp := new(big.Int).SetBytes(bytes)
	exp, token, err := session.Accept(rand.Reader, skey, exp)
	if err != nil {
		fmt.Printf("failed to accept incoming exchange: %v\n", err)
		close(out)
		return
	}
	trans <- exp.Bytes()
	trans <- token

	// Receive the foreign auth token and if verifies conclude session
	token = <-trans
	err = session.Finalize(pkey, token)
	if err != nil {
		fmt.Printf("failed to finalize exchange: %v\n", err)
		close(out)
		return
	}
	// Protocol done
	secret, err := session.Secret()
	if err != nil {
		fmt.Printf("failed to retrieve exchanged secret: %v\n", err)
		close(out)
		return
	}
	out <- secret
	return
}
