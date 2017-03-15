package eightsetmap

import "sort"

// MultiUnion returns the set of unique values associated to any of the given keys.
//
// NB this implementation is not fully optimized yet
func MultiUnion(m Map, keys ...uint64) []uint64 {
	vv := getSets(m, keys)
	if len(vv) == 0 {
		return []uint64{}
	}
	if len(vv) == 1 {
		return vv[0]
	}

	// start with the largest set as a base
	v3 := vv[len(vv)-1]
	for _, v2 := range vv[:len(vv)-1] {
		v3 = subUnion(v3, v2)
	}
	return v3
}

// MultiIntersect returns the set of values associated to all of the given keys.
// Note that if any keys are missing, or any pair has no intersection then the result is empty.
//
// NB this implementation is not fully optimized yet
func MultiIntersect(m Map, keys ...uint64) []uint64 {
	if len(keys) == 0 {
		return []uint64{}
	}
	vv := getSets(m, keys)
	if len(vv) != len(keys) {
		return []uint64{}
	}
	if len(keys) == 1 {
		return vv[0]
	}

	// start with the smallest 2 sets
	v3 := vv[0]
	for _, v2 := range vv[1:] {
		v3 = subIntersect(v3, v2)
		if len(v3) == 0 {
			// short-circuit if we can
			return []uint64{}
		}
	}
	return v3
}

// Union returns the set of unique values associated to either k1 or k2.
func Union(m Map, k1, k2 uint64) []uint64 {
	v1, ok := m.Get(k1)
	if !ok {
		// no k1, just return k2
		v2, _ := m.Get(k2)
		return v2
	}
	v2, ok := m.Get(k2)
	if !ok {
		// no k2, just return k1
		return v1
	}

	// merge-union both sorted sets
	return subUnion(v1, v2)
}

// Intersect returns the set of values associated to both k1 and k2.
func Intersect(m Map, k1, k2 uint64) []uint64 {
	v1, ok := m.Get(k1)
	if !ok {
		// no k1, just return empty
		return []uint64{}
	}
	v2, ok := m.Get(k2)
	if !ok {
		// no k2, just return empty
		return []uint64{}
	}

	// merge-intersect both sorted sets
	return subIntersect(v1, v2)
}

// Difference returns the set of values for k1 after removing any values also found in k2's set.
func Difference(m Map, k1, k2 uint64) []uint64 {
	v1, ok := m.Get(k1)
	if !ok {
		// no k1, just return empty
		return []uint64{}
	}
	v2, ok := m.Get(k2)
	if !ok {
		// no k2, just return k1
		return v1
	}

	// merge-difference both sorted sets
	v3 := make([]uint64, 0, len(v1))
	i, j := 0, 0
	for i < len(v1) || j < len(v2) {
		if j == len(v2) {
			// end of v2, just copy v1
			v3 = append(v3, v1[i])
			i++
			continue
		}

		if v1[i] <= v2[j] {
			if v1[i] == v2[j] {
				// match, remove it...
				j++
			} else {
				v3 = append(v3, v1[i])
			}
			i++
			continue
		}

		j++
	}
	return v3
}

//////////

func getSets(m Map, keys []uint64) [][]uint64 {
	vv := make([][]uint64, 0, len(keys))
	for _, k := range keys {
		v, ok := m.Get(k)
		if !ok || len(v) == 0 {
			continue
		}
		vv = append(vv, v)
	}
	// sort by size so algorithms can amortize costs
	sort.Slice(vv, func(i, j int) bool { return len(vv[i]) < len(vv[j]) })
	return vv
}

func subUnion(v1, v2 []uint64) []uint64 {
	v3 := make([]uint64, 0, len(v1)+len(v2))
	i, j := 0, 0
	for i < len(v1) || j < len(v2) {
		if i == len(v1) {
			// end of v1, just copy v2
			v3 = append(v3, v2[j])
			j++
			continue
		}
		if j == len(v2) {
			// end of v2, just copy v1
			v3 = append(v3, v1[i])
			i++
			continue
		}

		if v1[i] <= v2[j] {
			v3 = append(v3, v1[i])
			if v1[i] == v2[j] {
				j++
			}
			i++
			continue
		}

		v3 = append(v3, v2[j])
		j++
	}
	return v3
}

func subIntersect(v1, v2 []uint64) []uint64 {
	x := len(v1)
	if len(v2) < x {
		x = len(v2)
	}
	v3 := make([]uint64, 0, x)
	i, j := 0, 0
	for i < len(v1) && j < len(v2) {
		if v1[i] <= v2[j] {
			if v1[i] == v2[j] {
				v3 = append(v3, v1[i])
				j++
			}
			i++
		} else {
			j++
		}
	}
	return v3
}
