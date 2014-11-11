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
