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

package queue_test

import (
	"fmt"

	"github.com/project-iris/iris/container/queue"
)

// Simple usage example that inserts the numbers 0, 1, 2 into a queue and then
// removes them one by one, printing them to the standard output.
func Example_usage() {
	// Create a queue an push some data in
	q := queue.New()
	for i := 0; i < 3; i++ {
		q.Push(i)
	}
	// Pop out the queue contents and display them
	for !q.Empty() {
		fmt.Println(q.Pop())
	}
	// Output:
	// 0
	// 1
	// 2
}
