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

package mathext_test

import (
	"fmt"
	"math/big"

	"github.com/project-iris/iris/ext/mathext"
)

func ExampleMaxBigInt() {
	// Define some sample big ints
	four := big.NewInt(4)
	five := big.NewInt(5)

	// Print the minimum and maximum of the two
	fmt.Println("Min:", mathext.MinBigInt(four, five))
	fmt.Println("Max:", mathext.MaxBigInt(four, five))

	// Output:
	// Min: 4
	// Max: 5
}
