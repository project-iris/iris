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

// Package queue implements a FIFO (first in first out) data structure supporting
// arbitrary types (even a mixture).
//
// Internally it uses a dynamically growing circular slice of blocks, resulting
// in faster resizes than a simple dynamic array/slice would allow.
package queue

// The size of a block of data
const blockSize = 4096

// First in, first out data structure.
type Queue struct {
	tailIdx int
	headIdx int
	tailOff int
	headOff int

	blocks [][]interface{}
	head   []interface{}
	tail   []interface{}
}

// Creates a new, empty queue.
func New() *Queue {
	result := new(Queue)
	result.blocks = [][]interface{}{make([]interface{}, blockSize)}
	result.head = result.blocks[0]
	result.tail = result.blocks[0]
	return result
}

// Pushes a new element into the queue, expanding it if necessary.
func (q *Queue) Push(data interface{}) {
	q.tail[q.tailOff] = data
	q.tailOff++
	if q.tailOff == blockSize {
		q.tailOff = 0
		q.tailIdx = (q.tailIdx + 1) % len(q.blocks)

		// If we wrapped over to the end, insert a new block and update indices
		if q.tailIdx == q.headIdx {
			buffer := make([][]interface{}, len(q.blocks)+1)
			copy(buffer[:q.tailIdx], q.blocks[:q.tailIdx])
			buffer[q.tailIdx] = make([]interface{}, blockSize)
			copy(buffer[q.tailIdx+1:], q.blocks[q.tailIdx:])
			q.blocks = buffer
			q.headIdx++
			q.head = q.blocks[q.headIdx]
		}
		q.tail = q.blocks[q.tailIdx]
	}
}

// Pops out an element from the queue. Note, no bounds checking are done.
func (q *Queue) Pop() (res interface{}) {
	res, q.head[q.headOff] = q.head[q.headOff], nil
	q.headOff++
	if q.headOff == blockSize {
		q.headOff = 0
		q.headIdx = (q.headIdx + 1) % len(q.blocks)
		q.head = q.blocks[q.headIdx]
	}
	return
}

// Returns the first element in the queue. Note, no bounds checking are done.
func (q *Queue) Front() interface{} {
	return q.head[q.headOff]
}

// Checks whether the queue is empty.
func (q *Queue) Empty() bool {
	return q.headIdx == q.tailIdx && q.headOff == q.tailOff
}

// Returns the number of elements in the queue.
func (q *Queue) Size() int {
	if q.tailIdx > q.headIdx {
		return (q.tailIdx-q.headIdx)*blockSize - q.headOff + q.tailOff
	} else if q.tailIdx < q.headIdx {
		return (len(q.blocks)-q.headIdx+q.tailIdx)*blockSize - q.headOff + q.tailOff
	} else {
		return q.tailOff - q.headOff
	}
}

// Clears out the contents of the queue.
func (q *Queue) Reset() {
	// Rewind the queue indices
	q.headIdx = 0
	q.tailIdx = 0
	q.headOff = 0
	q.tailOff = 0

	// Reset the active blocks
	q.head = q.blocks[0]
	q.tail = q.blocks[0]

	// Set all elements to nil to allow garbage collection
	for _, block := range q.blocks {
		for i := 0; i < len(block); i++ {
			block[i] = nil
		}
	}
}
