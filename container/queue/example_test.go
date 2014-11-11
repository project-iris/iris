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
