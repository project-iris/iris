// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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

// Contains environmental modifications required if running on top of the Google
// Compute Engine platform. Specifically, GCE assignes /32 netmasks, crippling
// the bootstrapper. The checks here ensure that if Iris is on top of CGE, the
// masks are retrieved and updated from the CGE network setup.

package bootstrap

import (
	"io/ioutil"
	"net"
	"net/http"
	"path"

	"code.google.com/p/goauth2/compute/serviceaccount"
	"google.golang.org/api/compute/v1"
)

const gceMetadataUrl = "http://metadata.google.internal/computeMetadata/v1"

// Detects whether we are running on top of CGE.
func detectGoogleComputeEngine() bool {
	res, err := http.Get(gceMetadataUrl)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	return res.Header.Get("Metadata-Flavor") == "Google"
}

// Updates the network mask with the real on from the cloud config.
func updateIPNet(ipnet *net.IPNet) error {
	// Retrieve the configuration ids
	projectId, err := fetchProjectId()
	if err != nil {
		return err
	}
	networkId, err := fetchNetworkId()
	if err != nil {
		return err
	}
	// Create a service account connection to the GCE API
	client, err := serviceaccount.NewClient(&serviceaccount.Options{})
	if err != nil {
		return err
	}
	gce, err := compute.New(client)
	if err != nil {
		return err
	}
	// Fetch the network configurations and update the netmask
	network, err := gce.Networks.Get(projectId, networkId).Do()
	if err != nil {
		return err
	}
	_, ipn, err := net.ParseCIDR(network.IPv4Range)
	if err != nil {
		return err
	}
	ipnet.Mask = ipn.Mask
	return nil
}

// Fetches the project ID of the instance.
func fetchProjectId() (string, error) {
	return fetchMetadata("/project/project-id")
}

// Fetches the network ID in which the instance resides.
func fetchNetworkId() (string, error) {
	if res, err := fetchMetadata("/instance/network-interfaces/0/network"); err != nil {
		return "", err
	} else {
		return path.Base(res), nil
	}
}

// Fetches some metadata from the GCE metadata server.
func fetchMetadata(path string) (string, error) {
	// Construct the metadata request
	req, err := http.NewRequest("GET", gceMetadataUrl+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Metadata-Flavor", "Google")

	// Fetch the response
	res, err := new(http.Client).Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	// Read it all back and return
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
