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

package cyclic_test

import (
	"crypto/rand"
	"fmt"

	"github.com/project-iris/iris/crypto/cyclic"
)

func Example_usage() {
	// Generate the cyclic group
	group, err := cyclic.New(rand.Reader, 120)
	if err != nil {
		fmt.Println("Failed to generate cyclic group:", err)
	}
	// Output in a nice, source friendly byte format
	fmt.Println("Cyclic group base:")
	bytes := group.Base.Bytes()
	for byte := 0; byte < len(bytes); byte++ {
		fmt.Printf("0x%02x, ", bytes[byte])
		if byte%8 == 7 {
			fmt.Println()
		}
	}
	fmt.Println()
	// Output in a nice, source friendly byte format
	fmt.Println("Cyclic group generator:")
	bytes = group.Generator.Bytes()
	for byte := 0; byte < len(bytes); byte++ {
		fmt.Printf("0x%02x, ", bytes[byte])
		if byte%8 == 7 {
			fmt.Println()
		}
	}
	fmt.Println()
}
