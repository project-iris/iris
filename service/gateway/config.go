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
	"crypto/rand"
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
	Access    string   `json:"access,omitempty"`
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

// Adds a network to an existing gateway configuration. Note, that all arguments
// are expected to be pre-validated!
func AddNetwork(config []byte, net []byte, access string, gates []string, key *rsa.PrivateKey) ([]byte, error) {
	// Parse the gateway and network config blobs
	gateConf, err := parseGatewayConfig(config, &key.PublicKey)
	if err != nil {
		return nil, err
	}
	netConf, err := parseNetworkConfig(net)
	if err != nil {
		return nil, err
	}
	// Dedup networks
	for _, n := range gateConf.Networks {
		if n.Name == netConf.Name {
			return nil, fmt.Errorf("network already exists")
		}
	}
	// Calculate the remote network's public key signature
	hasher := sha256.New()
	if _, err := hasher.Write(netConf.Key); err != nil {
		return nil, err
	}
	sign, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, hasher.Sum(nil))
	if err != nil {
		return nil, err
	}
	netConf.Signature = sign

	// Set the outstanding variables, insert and marshal
	netConf.Access = access
	netConf.Gateways = gates

	gateConf.Networks = append(gateConf.Networks, *netConf)
	return marshalGatewayConfig(gateConf)
}

// Removes one or more networks from an existing gateway configuration.
func RemoveNetwork(config []byte, net string, key *rsa.PrivateKey) ([]byte, error) {
	// Parse the gateway config blob
	gateConf, err := parseGatewayConfig(config, &key.PublicKey)
	if err != nil {
		return nil, err
	}
	// Remove the network and marshal the new config
	found := false
	for i := 0; i < len(gateConf.Networks) && !found; i++ {
		if gateConf.Networks[i].Name == net {
			gateConf.Networks = append(gateConf.Networks[:i], gateConf.Networks[i+1:]...)
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("unknown network: \"%v\"", net)
	}
	return marshalGatewayConfig(gateConf)
}

// Parses a JSON configuration string, verifies all cryptographic keys and
// signatures against the node key.
func parseGatewayConfig(config []byte, key *rsa.PublicKey) (*Config, error) {
	// Parse the JSON structure
	conf := new(Config)
	if err := json.Unmarshal(config, conf); err != nil {
		return nil, err
	}
	// Verify all public keys
	for _, net := range conf.Networks {
		if _, err := x509.ParsePKIXPublicKey(net.Key); err != nil {
			return nil, err
		}
		// Verify the signature
		hasher := sha256.New()
		if _, err := hasher.Write(net.Key); err != nil {
			return nil, err
		}
		if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, hasher.Sum(nil), net.Signature); err != nil {
			return nil, err
		}
	}
	// Sanity checks on the config values
	// if len(conf.Networks) == 0 {
	// 	return nil, fmt.Errorf("no remote networks")
	// }
	// for _, net := range conf.Networks {
	// 	if len(net.Gateways) == 0 {
	// 		return nil, fmt.Errorf("no gateways for network %v", net.Name)
	// 	}
	// }
	return conf, nil
}

// Parses a JSON configuration string of a remote network stats.
func parseNetworkConfig(config []byte) (*Network, error) {
	// Parse the JSON structure
	conf := new(Network)
	if err := json.Unmarshal(config, conf); err != nil {
		return nil, err
	}
	// Sanity checks on some fields
	if conf.Name == "" {
		return nil, fmt.Errorf("missing name")
	}
	if conf.Key == nil {
		return nil, fmt.Errorf("missing key")
	}
	if _, err := x509.ParsePKIXPublicKey(conf.Key); err != nil {
		return nil, err
	}
	return conf, nil
}

// Marshals a gateway config structure into a JSON byte stream.
func marshalGatewayConfig(config *Config) ([]byte, error) {
	return json.MarshalIndent(config, "", "  ")
}
