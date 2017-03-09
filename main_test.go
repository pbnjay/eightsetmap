package eightsetmap

import (
	"log"
	"math/rand"
	"os"
	"testing"
)

func TestSimple(t *testing.T) {
	os.Remove("testing.8sm")
	m := New("testing.8sm")

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

	mm := m.Mutate(false)
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

	m2 := New("testing.8sm")
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

	os.Remove("testing.8sm")
}

func TestFibo(t *testing.T) {
	os.Remove("fibo_testing.8sm")
	m := New("fibo_testing.8sm")
	mm := m.Mutate(false)

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
	info, err := os.Stat("fibo_testing.8sm")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file")
	}
	sz := info.Size()

	m.Data = []byte("this is a random comment embedded in the file")
	err = mm.Commit(true)
	if err != nil {
		log.Println(err)
		t.Fatal("unable to commit packed changes")
	}
	info, err = os.Stat("fibo_testing.8sm")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file")
	}
	szPacked := info.Size()

	if szPacked > sz {
		t.Fatalf("packed file is %d bytes, but unpacked file is %d bytes", szPacked, sz)
	}
	//////////////
	mm = m.Mutate(false)

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
	info, err = os.Stat("fibo_testing.8sm")
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
	info, err = os.Stat("fibo_testing.8sm")
	if err != nil {
		log.Println(err)
		t.Fatal("unable to stat committed file (round2)")
	}
	szPacked = info.Size()
	if szPacked > sz {
		t.Fatalf("packed file is %d bytes, but unpacked file is %d bytes (round2)", szPacked, sz)
	}

	////////////////
	m2 := NewShifted("fibo_testing.8sm", 3)
	if len(m2.Data) == 0 {
		t.Fatal("embedded comment not preserved")
	}
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
	// check a few missing keys to test shifted scan miss path
	for _, f := range []uint64{611, 612, 613, 614, 615} {
		_, found := m2.Get(f)
		if found {
			t.Fatal("should not have found key for", f)
		}
	}
}

func TestInplace(t *testing.T) {
	os.Remove("inplace_testing.8sm")
	m := New("inplace_testing.8sm")
	mm := m.Mutate(false)

	justUnder := int(FillFactor) - 1
	mk1 := mm.OpenKey(42)
	mk2 := mm.OpenKey(43)
	mk3 := mm.OpenKey(44)
	mk4 := mm.OpenKey(45)
	for i := 0; i < justUnder; i++ {
		mk1.Put(42)
		mk2.Put(42 * uint64(i)) // duplicate of existing
		mk3.Put(uint64(i))      // new value in sequence
		mk4.Put(rand.Uint64())  // not in sequence
	}

	mk1.Sync()
	mk2.Sync()
	mk3.Sync()
	mk4.Sync()
	err := mm.Commit(false)
	if err != nil {
		t.Fatal("unable to commit changes", err)
	}

	vals, ok := m.Get(42)
	if !ok {
		t.Fatal("key 42 not found after adding!")
	}
	if len(vals) != 1 {
		t.Fatalf("found %d != 1 values in set", len(vals))
	}
	for _, k := range []uint64{43, 44, 45} {
		vals, ok = m.Get(k)
		if !ok {
			t.Fatalf("key %d not found after adding!", k)
		}
		if len(vals) != justUnder {
			t.Fatalf("found %d != %d values in set for key %d", len(vals), justUnder, k)
		}
	}
	info, err := os.Stat("inplace_testing.8sm")
	if err != nil {
		t.Fatal("unable to stat inplace_testing.8sm", err.Error())
	}
	sz := info.Size()

	/////////
	// open a new instance and add 3 values. filesize should stay the same
	m = New("inplace_testing.8sm")
	mm = m.Mutate(false)

	toadd := 1 + int(DefaultCapacity-FillFactor)/2
	mk1 = mm.OpenKey(42)
	mk2 = mm.OpenKey(43)
	mk3 = mm.OpenKey(44)
	mk4 = mm.OpenKey(45)
	for i := 0; i < toadd; i++ {
		mk1.Put(42)
		mk2.Put(42 * uint64(i))        // duplicate of existing
		mk3.Put(uint64(justUnder + i)) // new value in sequence
		mk4.Put(rand.Uint64())         // not in sequence
	}

	mk1.Sync()
	mk2.Sync()
	mk3.Sync()
	mk4.Sync()
	err = mm.Commit(false)
	if err != nil {
		t.Fatal("unable to commit changes", err)
	}

	vals, ok = m.Get(42)
	if !ok {
		t.Fatalf("key %d not found after adding!", 42)
	}
	if len(vals) != 1 {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), 1, 42)
	}

	vals, ok = m.Get(43)
	if !ok {
		t.Fatalf("key %d not found after adding!", 43)
	}
	if len(vals) != justUnder {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), justUnder, 43)
	}

	vals, ok = m.Get(44)
	if !ok {
		t.Fatalf("key %d not found after adding!", 44)
	}
	if len(vals) <= justUnder {
		t.Fatalf("found %d <= %d values in set for key %d", len(vals), justUnder, 44)
	}

	vals, ok = m.Get(45)
	if !ok {
		t.Fatalf("key %d not found after adding!", 45)
	}
	if len(vals) <= justUnder {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), justUnder, 45)
	}
	k4size := len(vals)

	info, err = os.Stat("inplace_testing.8sm")
	if err != nil {
		t.Fatal("unable to stat inplace_testing.8sm", err.Error())
	}
	sz2 := info.Size()
	if sz != sz2 {
		t.Fatalf("size changed. should have used in-place update (new:%d != old:%d)", sz2, sz)
	}

	/////////
	// open a new instance and remove 3 values. filesize should stay the same
	m = New("inplace_testing.8sm")
	mm = m.Mutate(false)

	mk2 = mm.OpenKey(43)
	mk3 = mm.OpenKey(44)
	mk4 = mm.OpenKey(45)
	for i := 0; i < toadd; i++ {
		mk2.Remove(42 * uint64(i))
		mk3.Remove(uint64(justUnder + i))
		mk4.Remove(rand.Uint64())
	}

	mk2.Sync()
	mk3.Sync()
	mk4.Sync()
	err = mm.Commit(false)
	if err != nil {
		t.Fatal("unable to commit changes", err)
	}

	vals, ok = m.Get(42)
	if !ok {
		t.Fatalf("key %d not found after removing!", 42)
	}
	if len(vals) != 1 {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), 1, 42)
	}

	vals, ok = m.Get(43)
	if !ok {
		t.Fatalf("key %d not found after adding!", 43)
	}
	if len(vals) != (justUnder - toadd) {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), (justUnder - toadd), 43)
	}

	vals, ok = m.Get(44)
	if !ok {
		t.Fatalf("key %d not found after adding!", 44)
	}
	if len(vals) != justUnder {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), justUnder, 44)
	}

	vals, ok = m.Get(45)
	if !ok {
		t.Fatalf("key %d not found after adding!", 45)
	}
	if len(vals) != k4size {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), k4size, 45)
	}

	info, err = os.Stat("inplace_testing.8sm")
	if err != nil {
		t.Fatal("unable to stat inplace_testing.8sm", err.Error())
	}
	sz2 = info.Size()
	if sz != sz2 {
		t.Fatalf("size changed. should have used in-place update (new:%d != old:%d)", sz2, sz)
	}

	//////////
	// open a new instance and add a lot of values. filesize should grow
	m = New("inplace_testing.8sm")
	mm = m.Mutate(false)

	mk4 = mm.OpenKey(45)
	for i := 0; i < 99*int(DefaultCapacity); i++ {
		mk4.Put(rand.Uint64())
	}

	mk4.Sync()
	err = mm.Commit(false)
	if err != nil {
		t.Fatal("unable to commit changes", err)
	}

	vals, ok = m.Get(42)
	if !ok {
		t.Fatalf("key %d not found after removing!", 42)
	}
	if len(vals) != 1 {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), 1, 42)
	}

	vals, ok = m.Get(43)
	if !ok {
		t.Fatalf("key %d not found after adding!", 43)
	}
	if len(vals) != (justUnder - toadd) {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), (justUnder - toadd), 43)
	}

	vals, ok = m.Get(44)
	if !ok {
		t.Fatalf("key %d not found after adding!", 44)
	}
	if len(vals) != justUnder {
		t.Fatalf("found %d != %d values in set for key %d", len(vals), justUnder, 44)
	}

	vals, ok = m.Get(45)
	if !ok {
		t.Fatalf("key %d not found after adding!", 45)
	}
	if len(vals) == k4size {
		t.Fatalf("found %d == %d values in set for key %d", len(vals), k4size, 45)
	}

	info, err = os.Stat("inplace_testing.8sm")
	if err != nil {
		t.Fatal("unable to stat inplace_testing.8sm", err.Error())
	}
	sz2 = info.Size()
	if sz == sz2 {
		t.Fatalf("size did not change. should have grown (new:%d != old:%d)", sz2, sz)
	}
}
