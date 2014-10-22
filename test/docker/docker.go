// Iris - Decentralized cloud messaging
// Copyright (c) 2014 Project Iris. All rights reserved.
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

// Package docker contains common operations on docker containers.
package docker

import (
	"bytes"
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
