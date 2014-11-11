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

package sortext

import (
	"sort"
	"testing"
)

type uniqueTest struct {
	data []int
	num  int
}

var uniqueTests = []uniqueTest{
	{[]int{}, 0},
	{[]int{1}, 1},
	{
		[]int{
			1,
			2, 2,
			3, 3, 3,
			4, 4, 4, 4,
			5, 5, 5, 5, 5,
			6, 6, 6, 6, 6, 6,
		},
		6,
	},
}

func TestUnique(t *testing.T) {
	for i, tt := range uniqueTests {
		n := Unique(sort.IntSlice(tt.data))
		if n != tt.num {
			t.Errorf("test %d: unique count mismatch: have %v, want %v.", i, n, tt.num)
		}
		for j := 0; j < n; j++ {
			for k := j + 1; k < n; k++ {
				if tt.data[j] >= tt.data[k] {
					t.Errorf("test %d: uniqueness violation: (%d, %d) in %v.", i, j, k, tt.data[:n])
				}
			}
		}
	}
}
