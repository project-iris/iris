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

package sortext

import (
	"sort"
)

// Unique gathers the first occurance of each element to the front, returning
// their number. Data must be sorted in ascending order. The order of the rest
// is ruined.
func Unique(data sort.Interface) int {
	n, u, i := data.Len(), 0, 1
	if n < 2 {
		return n
	}
	for i < n {
		if data.Less(u, i) {
			u++
			data.Swap(u, i)
		}
		i++
	}
	return u + 1
}
