package eightsetmap

import (
	"log"
	"math/rand"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	os.Remove("testing.bin")
	m := New("testing.bin")

	var x = int64(-1)
	for _, k := range []uint64{1, 42, 0, uint64(x)} {
		_, found := m.Get(k)
		if found {
			t.Fatal("found", k, "in empty test")
		}
		_, found = m.GetSet(k)
		if found {
			t.Fatal("found", k, "in empty test")
		}
	}

	mm := m.Mutate()
	mk := mm.OpenKey(1)
	mk.Put(10)
	mk.Sync()
	vals, found := mm.Get(1)
	if !found {
		t.Fatal("did not find 1 after adding")
	}
	if len(vals) != 1 {
		t.Fatal("found", len(vals), " values instead of 1 value after adding")
	}
	if vals[0] != 10 {
		t.Fatal("found v[0]=", vals[0], " != 10 after adding")
	}

	mk = mm.OpenKey(1)
	mk.Put(20)
	mk.Sync()
	vals, found = mm.Get(1)
	if !found {
		t.Fatal("did not find 1 after adding 2nd value")
	}
	if len(vals) != 2 {
		t.Fatal("found", len(vals), " values instead of 2 values after adding 2nd")
	}
	if vals[0] != 10 {
		t.Fatal("found v[0]=", vals[0], " != 10 after adding")
	}
	if vals[1] != 20 {
		t.Fatal("found v[1]=", vals[1], " != 20 after adding")
	}

	mk = mm.OpenKey(1)
	mk.Put(5)
	mk.Sync()
	vals, found = mm.Get(1)
	if !found {
		t.Fatal("did not find 1 after adding 3rd value")
	}
	if len(vals) != 3 {
		t.Fatal("found", len(vals), " values instead of 3 values after adding 3rd")
	}
	if vals[0] != 5 {
		t.Fatal("found v[0]=", vals[0], " != 5 after adding")
	}
	if vals[1] != 10 {
		t.Fatal("found v[1]=", vals[1], " != 10 after adding")
	}
	if vals[2] != 20 {
		t.Fatal("found v[2]=", vals[1], " != 20 after adding")
	}

	err := mm.Commit(true)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit changes")
	}
	/////////
	vals, found = m.Get(1)
	if !found {
		t.Fatal("did not find 1 after committing")
	}
	if len(vals) != 3 {
		t.Fatal("found", len(vals), " values instead of 3 values after committing")
	}
	if vals[0] != 5 {
		t.Fatal("found v[0]=", vals[0], " != 5 after committing")
	}
	if vals[1] != 10 {
		t.Fatal("found v[1]=", vals[1], " != 10 after committing")
	}
	if vals[2] != 20 {
		t.Fatal("found v[2]=", vals[1], " != 20 after committing")
	}

	m2 := New("testing.bin")
	vals, found = m2.Get(1)
	if !found {
		t.Fatal("did not find 1 after reopening")
	}
	if len(vals) != 3 {
		t.Fatal("found", len(vals), " values instead of 3 values after reopening")
	}
	if vals[0] != 5 {
		t.Fatal("found v[0]=", vals[0], " != 5 after reopening")
	}
	if vals[1] != 10 {
		t.Fatal("found v[1]=", vals[1], " != 10 after reopening")
	}
	if vals[2] != 20 {
		t.Fatal("found v[2]=", vals[1], " != 20 after reopening")
	}

	os.Remove("testing.bin")
}

func TestFibo(t *testing.T) {
	os.Remove("fibo_testing.bin")
	m := New("fibo_testing.bin")
	mm := m.Mutate()

	fibs := []uint64{
		0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181, 6765,
	}

	for _, f := range fibs {
		_, found := mm.Get(f)
		if found {
			t.Fatal("found", f, "before it was added")
		}
		mk := mm.OpenKey(f)
		if f%2 == 0 {
			for i := uint64(0); i < f; i++ {
				mk.Put(rand.Uint64())
			}
		} else {
			for i := uint64(0); i < f; i++ {
				mk.Put(uint64(i))
			}
		}
		mk.Sync()
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

	err := mm.Commit(false)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit unpacked changes")
	}
	info, err := os.Stat("fibo_testing.bin")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file")
	}
	sz := info.Size()

	err = mm.Commit(true)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit packed changes")
	}
	info, err = os.Stat("fibo_testing.bin")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file")
	}
	szPacked := info.Size()
	log.Printf("packed file is %d bytes, unpacked file is %d bytes", szPacked, sz)

	if szPacked > sz {
		t.Fatalf("packed file is %d bytes, but unpacked file is %d bytes", szPacked, sz)
	}
	//////////////
	mm = m.Mutate()

	fibs2 := []uint64{
		4, 6, 7, 9, 10, 11, 12, 14, 15, 16, 17, 18, 19, 20, 3400, 5500, 8900, 144000, 2330000,
	}
	for _, f := range fibs2 {
		_, found := mm.Get(f)
		if found {
			t.Fatal("found", f, "before it was added (round2)")
		}
		mk := mm.OpenKey(f)
		if f%2 == 0 {
			for i := uint64(0); i < f; i++ {
				mk.Put(rand.Uint64())
			}
		} else {
			for i := uint64(0); i < f; i++ {
				mk.Put(uint64(i))
			}
		}
		mk.Sync()
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

	err = mm.Commit(false)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit unpacked changes (round2)")
	}
	info, err = os.Stat("fibo_testing.bin")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file (round2)")
	}
	sz = info.Size()

	err = mm.Commit(true)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit packed changes (round2)")
	}
	info, err = os.Stat("fibo_testing.bin")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file (round2)")
	}
	szPacked = info.Size()
	log.Printf("packed file is %d bytes, unpacked file is %d bytes (round2)", szPacked, sz)
	if szPacked > sz {
		t.Fatalf("packed file is %d bytes, but unpacked file is %d bytes (round2)", szPacked, sz)
	}

	////////////////
	m2 := NewShifted("fibo_testing.bin", 1)
	for _, f := range fibs {
		vals, found := m2.Get(f)
		if !found {
			t.Fatal("did not find", f, "when shifted")
		}
		if uint64(len(vals)) != f {
			t.Fatal("found", len(vals), " values instead of", f, "values after shifting")
		}
	}
	for _, f := range fibs2 {
		vals, found := m2.Get(f)
		if !found {
			t.Fatal("did not find", f, "when shifted")
		}
		if uint64(len(vals)) != f {
			t.Fatal("found", len(vals), " values instead of", f, "values after shifting")
		}
	}
}
