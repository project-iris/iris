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

package gateway

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
)

type Config struct {
	Address  string    `json:"address"`
	Networks []Network `json:"networks"`
}

type Network struct {
	Name      string   `json:"name"`
	Key       []byte   `json:"key"`
	Signature []byte   `json:"signature,omitempty"`
	Gateways  []string `json:"gateways"`
	Allow     string   `json:"allow,omitempty"`
}

// Initializes an empty gateway configuration blob, ready to accept remote
// network insertions.
func InitGatewayConfig(addr *net.TCPAddr) ([]byte, error) {
	config := &Config{
		Address:  addr.String(),
		Networks: []Network{},
	}
	return json.MarshalIndent(config, "", "  ")
}

// Initializes an empty remote network configuration blob, ready to be inserted
// into a gateway config.
func InitNetworkConfig(net string, key *rsa.PrivateKey) ([]byte, error) {
	// Extract the public key data
	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, err
	}
	// Create and marshal the configurations
	config := &Network{
		Name:     net,
		Key:      pub,
		Gateways: []string{},
	}
	return json.MarshalIndent(config, "", "  ")
}

// Parses a JSON configuration string, verifies all cryptographic keys and
// signatures against the node key and executes some sanity checks. If all
// pass, the configuration structure is returned.
func Parse(config []byte, key *rsa.PublicKey) (*Config, error) {
	// Parse the JSON structure
	conf := new(Config)
	if err := json.Unmarshal(config, conf); err != nil {
		return nil, err
	}
	// Verify all public keys
	for _, net := range conf.Networks {
		hasher := sha256.New()
		if _, err := hasher.Write(net.Key); err != nil {
			return nil, err
		}
		if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, hasher.Sum(nil), net.Signature); err != nil {
			return nil, err
		}
	}
	// Sanity checks on the config values
	if len(conf.Networks) == 0 {
		return nil, fmt.Errorf("no remote networks")
	}
	for _, net := range conf.Networks {
		if len(net.Gateways) == 0 {
			return nil, fmt.Errorf("no gateways for network %v", net.Name)
		}
	}
	return conf, nil
}
