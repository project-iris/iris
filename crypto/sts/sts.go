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

// Package sts implements the Station-to-station (STS) key exchange protocol.
//   Wikipedia: http://en.wikipedia.org/wiki/Station-to-Station_protocol
//   Diagram: http://goo.gl/5EiDV
//
// Although STS is a generic key exchange protocol, some assumptions were hard
// coded into the implementation:
//   The asymmetric signature algorithm is RSA
//   The symmetric encryption uses CTR mode
//   The stream crypto key and IV are expanded with HKDF from the master key
//
// The cryptographic strength of the protocol is based on the analysis of the
// general number field sieve algorithm, it being the fastest factoring method
// till now. Matching STS bit sizes to AES (approx!):
//   AES-128: 2248 bits
//   AES-192: 5912 bits
//   AES-256: 11920 bits
package sts

import (
	"crypto"
	"crypto/cipher"
	"crypto/rsa"
	"errors"
	"hash"
	"io"
	"math/big"

	"code.google.com/p/go.crypto/hkdf"
)

// Current step in the protocol to prevent user errors
type state uint8

const (
	created state = iota
	initiated
	accepted
	verified
	finalized
)

// Protocol state structure
type Session struct {
	state state

	group     *big.Int
	generator *big.Int

	exponent   *big.Int
	localExp   *big.Int
	foreignExp *big.Int
	secret     *big.Int

	hash    crypto.Hash
	crypter func([]byte) (cipher.Block, error)
	keybits int
}

// Ensure unique key expansion for STS
var hkdfSalt = []byte("crypto.sts.hkdf.salt")
var hkdfInfo = []byte("crypto.sts.hkdf.info")

// Creates a new STS session, ready to initiate or accept key exchanges. The group/generator pair defines the
// cyclic group on which STS will operate. cipher and bits are used during the authentication token's symmetric
// encryption, whilst hash is needed during RSA signing.
func New(random io.Reader, group, generator *big.Int, cipher func([]byte) (cipher.Block, error),
	bits int, hash crypto.Hash) (*Session, error) {
	// Generate a random secret exponent
	expbits := group.BitLen()
	secret := make([]byte, (expbits+7)/8)
	n, err := io.ReadFull(random, secret)
	if n != len(secret) || err != nil {
		return nil, err
	}

	// Clear top bits if non-power was requested
	clear := uint(expbits % 8)
	if clear == 0 {
		clear = 8
	}
	secret[0] &= uint8(int(1<<clear) - 1)

	exp := new(big.Int).SetBytes(secret)
	ses := new(Session)
	ses.group = group
	ses.generator = generator
	ses.exponent = exp
	ses.hash = hash
	ses.crypter = cipher
	ses.keybits = bits

	return ses, nil
}

// Initiates an STS exchange session, returning the local exponential to connect with.
func (s *Session) Initiate() (*big.Int, error) {
	// Sanity check
	if s.state != created {
		return nil, errors.New("only a new session can initiate key exchanges")
	}
	s.localExp = new(big.Int).Exp(s.generator, s.exponent, s.group)
	s.state = initiated
	return s.localExp, nil
}

// Accepts an incoming STS exchange session, returning the local exponential and the authorization token. The key is
// used to authenticate the token for teh other side, whilst the exp is the foreign exponential.
func (s *Session) Accept(random io.Reader, key *rsa.PrivateKey, exp *big.Int) (*big.Int, []byte, error) {
	// Sanity check
	if s.state != created {
		return nil, nil, errors.New("only a new session can accept key exchange requests")
	}
	s.localExp = new(big.Int).Exp(s.generator, s.exponent, s.group)
	s.foreignExp = exp
	s.secret = new(big.Int).Exp(exp, s.exponent, s.group)

	token, err := s.genToken(random, key)
	if err != nil {
		return nil, nil, err
	}
	s.state = accepted
	return s.localExp, token, nil
}

// Verifies the authenticity of a remote STS acceptor and returns the local auth token if successful. The exp is the
// foreign exponential used in calculating the token. pkey is used to verify the foreign signature whilst skey to
// generate the local signature.
func (s *Session) Verify(random io.Reader, skey *rsa.PrivateKey, pkey *rsa.PublicKey,
	exp *big.Int, token []byte) ([]byte, error) {
	// Sanity check
	if s.state != initiated {
		return nil, errors.New("only an initiated session can verify the acceptor")
	}
	// Verify the authorization token
	s.foreignExp = exp
	s.secret = new(big.Int).Exp(exp, s.exponent, s.group)
	err := s.verToken(pkey, token)
	if err != nil {
		return nil, err
	}
	// Generate this side's authorization token
	token, err = s.genToken(random, skey)
	if err != nil {
		return nil, err
	}
	s.state = verified
	return token, nil
}

// Finalizes an STS key exchange by authenticating the initiator's token with the local public key. Returns nil error
// if verification succeeded.
func (s *Session) Finalize(key *rsa.PublicKey, token []byte) error {
	// Sanity check
	if s.state != accepted {
		return errors.New("only an initiated session can verify the acceptor")
	}
	// Verify the authorization token
	err := s.verToken(key, token)
	if err != nil {
		return err
	}
	s.state = finalized
	return nil
}

// Retrieves the shared secret that the communicating parties agreed upon.
func (s *Session) Secret() ([]byte, error) {
	if s.state != verified && s.state != finalized {
		return nil, errors.New("only a verified or finalized session can return a reliable shared secret")
	}
	return s.secret.Bytes(), nil
}

// Calculates the authorization token: the encrypted RSA signature of the two exponentials (local first!)
func (s *Session) genToken(random io.Reader, key *rsa.PrivateKey) ([]byte, error) {
	// Calculate the RSA signature
	hasher := s.hash.New()
	hasher.Write(append(s.localExp.Bytes(), s.foreignExp.Bytes()...))
	hashsum := hasher.Sum(nil)
	sig, err := rsa.SignPKCS1v15(random, key, s.hash, hashsum)
	if err != nil {
		return nil, err
	}
	// Create a stream cipher and encrypt the RSA signature
	stream, err := s.makeCipher()
	if err != nil {
		return nil, err
	}
	stream.XORKeyStream(sig, sig)
	return sig, nil
}

// Verify the authorization token: the encrypted RSA signature of the two exponentials (foreign first!)
func (s *Session) verToken(key *rsa.PublicKey, token []byte) error {
	// Calculate the required hash sum
	hasher := s.hash.New()
	hasher.Write(append(s.foreignExp.Bytes(), s.localExp.Bytes()...))
	hashsum := hasher.Sum(nil)

	// Create the stream cipher and decrypt the RSA signature
	stream, err := s.makeCipher()
	if err != nil {
		return err
	}
	stream.XORKeyStream(token, token)

	// Verify the signature
	return rsa.VerifyPKCS1v15(key, s.hash, hashsum, token)
}

// Extracts a usable sized symmetric key and IV for the stream cipher from the huge master key, and
// creates a CTR stream cipher.
func (s *Session) makeCipher() (cipher.Stream, error) {
	// Create the key derivation function
	hasher := func() hash.Hash { return s.hash.New() }
	hkdf := hkdf.New(hasher, s.secret.Bytes(), hkdfSalt, hkdfInfo)

	// Extract the symmetric key
	key := make([]byte, s.keybits/8)
	n, err := io.ReadFull(hkdf, key)
	if n != len(key) || err != nil {
		return nil, err
	}
	// Create the block cipher
	block, err := s.crypter(key)
	if err != nil {
		return nil, err
	}
	// Extract the IV for the counter mode
	iv := make([]byte, block.BlockSize())
	n, err = io.ReadFull(hkdf, iv)
	if n != len(iv) || err != nil {
		return nil, err
	}
	// Create the stream cipher
	return cipher.NewCTR(block, iv), nil
}
