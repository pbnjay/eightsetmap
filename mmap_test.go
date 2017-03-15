package eightsetmap

import "testing"

func TestMMap(t *testing.T) {
	m := New("fibo_testing.8sm")
	mm := MMap(m)

	fibs := []uint64{
		0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765,
	}

	fibs2 := []uint64{
		4, 6, 7, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 3400, 5500, 8900, 144000, 2330000,
	}
	for _, f := range fibs {
		vals, found := mm.Get(f)
		if !found {
			t.Fatal("did not find", f, "after adding")
		}
		if uint64(len(vals)) != f {
			t.Fatal("found", len(vals), " values instead of", f, "values after adding")
		}
		if f%2 == 1 {
			for i, x := range vals {
				if uint64(i) != x {
					t.Fatalf("found v[%d]=%d != %d after adding", i, x, i)
				}
			}
		} else {
			for i, x := range vals {
				if i > 0 && vals[i-1] > x {
					t.Fatalf("values sort invariant not held after adding")
				}
			}
		}
	}

	for _, f := range fibs2 {
		vals, found := mm.Get(f)
		if !found {
			t.Fatal("did not find", f, "after adding (round2)")
		}
		if uint64(len(vals)) != f {
			t.Fatal("found", len(vals), " values instead of", f, "values after adding (round2)")
		}
		if f%2 == 1 {
			for i, x := range vals {
				if uint64(i) != x {
					t.Fatalf("found v[%d]=%d != %d after adding (round2)", i, x, i)
				}
			}
		} else {
			for i, x := range vals {
				if i > 0 && vals[i-1] > x {
					t.Fatalf("values sort invariant not held after adding (round2)")
				}
			}
		}
	}
}
