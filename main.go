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

package main

import (
	"fmt"
	"os"
)

func main() {
	// Extract the subcommand if any was specified
	cmd := "iris"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	// Switch logic based on command
	switch cmd {
	case "gateway":
		// Fetch the subcommand
		subCmd := ""
		if len(os.Args) > 2 {
			subCmd = os.Args[2]
		}
		// Switch the entry based on the gateway subcommand
		switch subCmd {
		case "init":
			gateInitMain()
		case "add":
			gateAddMain()
		case "rm":
			gateRmMain()
		default:
			fmt.Fprintf(os.Stderr, "invalid subcommand \"%s\" for command gateway\n", subCmd)
			os.Exit(1)
		}
	default:
		irisMain()
	}
}
