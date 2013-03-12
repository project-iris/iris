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

// Station-to-station key exchange protocol implementation. Details:
//   * Wikipedia: http://en.wikipedia.org/wiki/Station-to-Station_protocol
//   * Diagram: http://goo.gl/5EiDV
//
// Although STS is a generic key exchange protocol, some assumptions were hard
// coded into the implementation:
//   - The asymmetric signature algorithm is RSA
//   - The symmetric encryption uses CTR mode
//   - The stream crypto key and IV are expanded with HKDF from the master key
//
// The cryptographic strength of the protocol is based on the analysis of the
// general number field sieve algorithm, it being the fastest factoring method
// till now. Matching STS bit sizes to AES (approx!):
//   * AES-128: 2248 bits
//   * AES-192: 5912 bits
//   * AES-256: 11920 bits
package sts

import (
	"crypto"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rsa"
	"errors"
	"io"
	"math/big"
)

// Current step in the protocol to prevent user errors
type state uint8

const (
	NEW state = iota
	INITIATED
	ACCEPTED
	VERIFIED
	FINALIZED
)

// Protocol state structure
type session struct {
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
	bits int, hash crypto.Hash) (*session, error) {
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
	secret[0] &= uint8(int(1<<(clear%8)) - 1)

	exp := new(big.Int).SetBytes(secret)
	ses := new(session)

	ses.group = group
	ses.generator = generator
	ses.exponent = exp
	ses.hash = hash
	ses.crypter = cipher
	ses.keybits = bits

	return ses, nil
}

// Initiates an STS exchange session, returning the local exponential to connect with.
func (s *session) Initiate() (*big.Int, error) {
	// Sanity check
	if s.state != NEW {
		return nil, errors.New("only a new session can initiate key exchanges")
	}
	s.localExp = new(big.Int).Exp(s.generator, s.exponent, s.group)
	s.state = INITIATED
	return s.localExp, nil
}

// Accepts an incoming STS exchange session, returning the local exponential and the authorization token. The key is
// used to authenticate the token for teh other side, whilst the exp is the foreign exponential.
func (s *session) Accept(random io.Reader, key *rsa.PrivateKey, exp *big.Int) (*big.Int, []byte, error) {
	// Sanity check
	if s.state != NEW {
		return nil, nil, errors.New("only a new session can accept key exchange requests")
	}
	s.localExp = new(big.Int).Exp(s.generator, s.exponent, s.group)
	s.foreignExp = exp
	s.secret = new(big.Int).Exp(exp, s.exponent, s.group)

	token, err := s.genToken(random, key)
	if err != nil {
		return nil, nil, err
	}
	s.state = ACCEPTED
	return s.localExp, token, nil
}

// Verifies the authenticity of a remote STS acceptor and returns the local auth token if successful. The exp is the
// foreign exponential used in calculating the token. pkey is used to verify the foreign signature whilst skey to
// generate the local signature.
func (s *session) Verify(random io.Reader, skey *rsa.PrivateKey, pkey *rsa.PublicKey,
	exp *big.Int, token []byte) ([]byte, error) {
	// Sanity check
	if s.state != INITIATED {
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
	s.state = VERIFIED
	return token, nil
}

// Finalizes an STS key exchange by authenticating the initiator's token with the local public key. Returns nil error
// if verification succeeded.
func (s *session) Finalize(key *rsa.PublicKey, token []byte) error {
	// Sanity check
	if s.state != ACCEPTED {
		return errors.New("only an initiated session can verify the acceptor")
	}
	// Verify the authorization token
	err := s.verToken(key, token)
	if err != nil {
		return err
	}
	s.state = FINALIZED
	return nil
}

// Retrieves the shared secret that the communicating parties agreed upon.
func (s *session) Secret() ([]byte, error) {
	if s.state != VERIFIED && s.state != FINALIZED {
		return nil, errors.New("only a verified or finalized session can return a reliable shared secret")
	}
	return s.secret.Bytes(), nil
}

// Calculates the authorization token: the encrypted RSA signature of the two exponentials (local first!)
func (s *session) genToken(random io.Reader, key *rsa.PrivateKey) ([]byte, error) {
	// Calculate the RSA signature
	hasher := s.hash.New()
	hasher.Write(append(s.localExp.Bytes(), s.foreignExp.Bytes()...))
	hashsum := hasher.Sum(nil)
	sig, err := rsa.SignPKCS1v15(random, key, s.hash, hashsum)
	if err != nil {
		return nil, err
	}
	// Extract a usable symmetric key and IV for the stream cipher
	symkey, iv, err := s.makeKeys()
	if err != nil {
		return nil, err
	}
	// Encrypt the signature
	block, err := s.crypter(symkey)
	if err != nil {
		return nil, err
	}
	crypter := cipher.NewCTR(block, iv)
	crypter.XORKeyStream(sig, sig)
	return sig, nil
}

// Verify the authorization token: the encrypted RSA signature of the two exponentials (foreign first!)
func (s *session) verToken(key *rsa.PublicKey, token []byte) error {
	// Calculate the required hash sum
	hasher := s.hash.New()
	hasher.Write(append(s.foreignExp.Bytes(), s.localExp.Bytes()...))
	hashsum := hasher.Sum(nil)

	// Extract a usable symmetric key and IV for the stream cipher
	symkey, iv, err := s.makeKeys()
	if err != nil {
		return err
	}
	// Decrypt the RSA signature
	block, err := s.crypter(symkey)
	if err != nil {
		return err
	}
	crypter := cipher.NewCTR(block, iv)
	crypter.XORKeyStream(token, token)

	// Verify the signature
	return rsa.VerifyPKCS1v15(key, s.hash, hashsum, token)
}

// Extracts a usable sized symmetric key and IV for the stream cipher from the huge master key
func (s *session) makeKeys() ([]byte, []byte, error) {
	hkdf := hkdf.New(s.hash, s.secret.Bytes(), hkdfSalt, hkdfInfo)
	symkey := make([]byte, s.keybits/8)
	n, err := io.ReadFull(hkdf, symkey)
	if n != len(symkey) || err != nil {
		return nil, nil, err
	}
	iv := make([]byte, s.keybits/8)
	n, err = io.ReadFull(hkdf, iv)
	if n != len(iv) || err != nil {
		return nil, nil, err
	}
	return symkey, iv, nil
}
