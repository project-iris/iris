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

// Package docker contains common operations on docker containers.
package docker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Docker command to invoke
const docker = "docker"

// Checks whether a docker installation can be found.
func CheckInstallation() error {
	_, err := exec.LookPath(docker)
	return err
}

// Checks whether a required docker image is available locally.
func CheckImage(image string) (bool, error) {
	out, err := exec.Command(docker, "images", "--no-trunc").Output()
	if err != nil {
		return false, err
	}
	return bytes.Contains(out, []byte(image)), nil
}

// Pulls an image from the docker registry.
func PullImage(image string) error {
	cmd := exec.Command(docker, "pull", image)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr

	return cmd.Run()
}

// Starts a container from an image, returning its id.
func StartContainer(args []string, image string, params ...string) (string, error) {
	engine := append([]string{"run", "-d"}, args...)
	program := append([]string{image}, params...)

	out, err := exec.Command(docker, append(engine, program...)...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// Forcefully terminates a docker container.
func CloseContainer(container string) error {
	return exec.Command(docker, "rm", "-f", container).Run()
}

// Retrieves the IP address of a running container.
func FetchIPAddress(container string) (string, error) {
	// Some temporary types to extract the IP address
	type networkSettings struct {
		IPAddress string
	}
	type containerSettings struct {
		NetworkSettings networkSettings
	}
	// Fetch the container settings and extract the IP path
	out, err := exec.Command(docker, "inspect", container).Output()
	if err != nil {
		return "", err
	}
	var containers []containerSettings
	if err := json.Unmarshal(out, &containers); err != nil {
		return "", err
	}
	// Return the IP, or fail with some reason
	if count := len(containers); count != 1 {
		return "", fmt.Errorf("invalid inspection count: %v", count)
	}
	if ip := containers[0].NetworkSettings.IPAddress; ip != "" {
		return ip, nil
	}
	return "", errors.New("IP not found")
}
