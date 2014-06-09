// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
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
