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
package cyclic_test

import (
	"crypto/cyclic"
	"crypto/rand"
	"fmt"
)

func Example_usage() {
	// Generate the cyclic group
	group, err := cyclic.New(rand.Reader, 512)
	if err != nil {
		fmt.Println("Failed to generate cyclic group:", err)
	}
	// Output in a nice, source friendly byte format
	fmt.Println("Cyclic group base:")
	bytes := group.Base.Bytes()
	for row := 0; row < len(bytes)/8; row++ {
		for col := 0; col < 8; col++ {
			fmt.Printf("0x%02x, ", bytes[8*row+col])
		}
		fmt.Println()
	}
	// Output in a nice, source friendly byte format
	fmt.Println("Cyclic group generator:")
	bytes = group.Generator.Bytes()
	for row := 0; row < len(bytes)/8; row++ {
		for col := 0; col < 8; col++ {
			fmt.Printf("0x%02x, ", bytes[8*row+col])
		}
		fmt.Println()
	}
}
