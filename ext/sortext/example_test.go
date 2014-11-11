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

package sortext_test

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/project-iris/iris/ext/sortext"
)

func ExampleBigInts() {
	// Define some sample big ints
	one := big.NewInt(1)
	two := big.NewInt(2)
	three := big.NewInt(3)
	four := big.NewInt(4)
	five := big.NewInt(5)
	six := big.NewInt(6)

	// Sort and print a random slice
	s := []*big.Int{five, two, six, three, one, four}
	sortext.BigInts(s)
	fmt.Println(s)

	// Output:
	// [1 2 3 4 5 6]
}

func ExampleUnique() {
	// Create some array of data
	data := []int{1, 5, 4, 3, 1, 3, 2, 5, 4, 3, 3, 0, 0}

	// Sort it
	sort.Ints(data)

	// Get unique elements and siplay them
	n := sortext.Unique(sort.IntSlice(data))
	fmt.Println("Uniques:", data[:n])

	// Output:
	// Uniques: [0 1 2 3 4 5]
}
