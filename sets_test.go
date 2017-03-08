package eightsetmap

import "testing"

func TestSets(t *testing.T) {
	all := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	evens := []uint64{2, 4, 6, 8, 10, 12, 14, 16}
	odds := []uint64{1, 3, 5, 7, 9, 11, 13, 15}
	fibs := []uint64{1, 2, 3, 5, 8, 13}
	efibs := []uint64{2, 8}
	ofibs := []uint64{1, 3, 5, 13}

	m := New("sets_testing.8sm")
	mm := m.Mutate()
	mk1 := mm.OpenKey(1)
	mk2 := mm.OpenKey(2)
	mk3 := mm.OpenKey(3)
	mk4 := mm.OpenKey(4)
	mk1.PutSlice(all)
	mk2.PutSlice(evens)
	mk3.PutSlice(odds)
	mk4.PutSlice(fibs)
	mk1.Sync()
	mk2.Sync()
	mk3.Sync()
	mk4.Sync()
	mm.Commit(true)

	chk := func(msg string, rs, ex []uint64) {
		if len(rs) != len(ex) {
			t.Fatalf("%s. size mismatch got %d, expected %d", msg, len(rs), len(ex))
		}
		for i, x := range rs {
			if x != ex[i] {
				t.Fatalf("%s. got result[%d]=%d, expected %d", i, x, ex[i])
			}
		}
	}

	result := m.Union(2, 3)
	chk("union of evens and odds", result, all)
	result = m.Union(1, 3)
	chk("union of all and odds", result, all)
	result = m.Union(3, 1)
	chk("union of odds and all", result, all)
	result = m.Union(2, 1)
	chk("union of evens and all", result, all)
	result = m.Union(1, 2)
	chk("union of all and evens", result, all)

	result = m.Union(2, 42)
	chk("union of evens and nothing", result, evens)
	result = m.Union(42, 3)
	chk("union of nothing and odds", result, odds)

	result = m.Intersect(2, 3)
	chk("intersection of evens and odds", result, []uint64{})
	result = m.Intersect(1, 3)
	chk("intersection of all and odds", result, odds)
	result = m.Intersect(2, 1)
	chk("intersection of evens and all", result, evens)

	result = m.Intersect(2, 4)
	chk("intersection of evens and fibs", result, efibs)
	result = m.Intersect(3, 4)
	chk("intersection of odds and fibs", result, ofibs)

	result = m.Intersect(2, 42)
	chk("intersection of evens and nothing", result, []uint64{})
	result = m.Intersect(42, 3)
	chk("intersection of nothing and odds", result, []uint64{})

	result = m.Difference(2, 3)
	chk("difference of evens - odds", result, evens)
	result = m.Difference(1, 3)
	chk("difference of all - odds", result, evens)
	result = m.Difference(2, 1)
	chk("difference of evens - all", result, []uint64{})

	result = m.Difference(2, 42)
	chk("difference of evens - nothing", result, evens)

	result = m.Difference(42, 2)
	chk("difference of nothing - evens", result, []uint64{})

	result = m.Difference(2, 4)
	chk("difference of evens - fibs", result, []uint64{4, 6, 10, 12, 14, 16})
	result = m.Difference(3, 4)
	chk("difference of odds - fibs", result, []uint64{7, 9, 11, 15})

	//////

	result = m.MultiUnion(1, 2, 3, 4)
	chk("multi-union of all, evens, odds, and fibs", result, all)
	result = m.MultiUnion(2, 3, 4)
	chk("multi-union of evens, odds, and fibs", result, all)
	result = m.MultiUnion(2, 3, 42)
	chk("multi-union of evens, odds, fibs, and nothing", result, all)
	result = m.MultiUnion(2, 3)
	chk("multi-union of evens and odds", result, all)

	result = m.MultiIntersect(1, 2, 3, 4)
	chk("multi-intersection of all, evens, odds, and fibs", result, []uint64{})
	result = m.MultiIntersect(2, 3, 4)
	chk("multi-intersection of evens, odds, and fibs", result, []uint64{})
	result = m.MultiIntersect(2, 3, 42)
	chk("multi-intersection of evens, odds, and nothing", result, []uint64{})
	result = m.MultiIntersect(2, 3)
	chk("multi-intersection of evens and odds", result, []uint64{})

	result = m.MultiIntersect(1, 2, 4)
	chk("multi-intersection of all, evens, and fibs", result, efibs)
	result = m.MultiIntersect(2, 1, 4)
	chk("multi-intersection of evens, all, and fibs", result, efibs)
	result = m.MultiIntersect(4, 2, 1)
	chk("multi-intersection of fibs, evens, and all", result, efibs)
	result = m.MultiIntersect(3, 4)
	chk("multi-intersection of odds and fibs", result, ofibs)

	////

	result = m.MultiIntersect(2)
	chk("multi-intersection of evens", result, evens)
	result = m.MultiIntersect(42)
	chk("multi-intersection of invalid key", result, []uint64{})
	result = m.MultiIntersect()
	chk("multi-intersection of nothing", result, []uint64{})

	result = m.MultiUnion(2)
	chk("multi-union of evens", result, evens)
	result = m.MultiUnion(42)
	chk("multi-union of invalid key", result, []uint64{})
	result = m.MultiUnion()
	chk("multi-union of nothing", result, []uint64{})

}

func TestMultiSets(t *testing.T) {

}
