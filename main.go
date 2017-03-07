package eightsetmap

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/hashicorp/golang-lru"
)

////////
//
// uint64 [num_keys]
// [num_keys][2]uint64 [key, offset]  (sorted by key)
//
// uint64 caplen [uint32 capacity, uint32 length]
// [capacity]uint64 with first [length]uint64 sorted values
//
////////

// Map represents a out-of-core map from uint64 keys to sets of uint64 values.
type Map struct {
	filename string
	f        *os.File // readonly file

	// 1 billion keys here will easily take over 16gb...
	offsets  map[uint64]int64
	shiftkey uint64

	//cache map[uint64][]uint64
	cache *lru.Cache
}

// MutableMap represents a Map that can be written to.
type MutableMap struct {
	*Map

	// not yet committed to disk
	dirty map[uint64][]uint64
}

// New returns a new Map backed by the (possibly empty) data in filename.
func New(filename string) *Map {
	return NewShifted(filename, 0)
}

// NewShifted returns a Map with shifting enabled to reduce core memory usage.
// A shift is a power of 2 factor, so shift=1 means that memory usage is
// approximately cut in half, but that lookups may take additional disk seeks.
func NewShifted(filename string, shift uint64) *Map {
	offs := make(map[uint64]int64)
	f, err := os.Open(filename)
	if err == nil {
		var i, n, key, lastkey uint64
		var o int64
		// number of offsets
		err = binary.Read(f, binary.LittleEndian, &n)
		if err != nil {
			panic(err)
		}
		for i = 0; i < n; i++ {
			// uint64 key
			err = binary.Read(f, binary.LittleEndian, &key)
			if err != nil {
				panic(err)
			}

			// int64 offset
			err = binary.Read(f, binary.LittleEndian, &o)
			if err != nil {
				panic(err)
			}

			if shift != 0 {
				if key < lastkey {
					panic("keys are not sorted! cannot use shift until repacked")
				}

				key >>= shift
				if _, exists := offs[key]; !exists {
					// gets the first table index, not the actual offset!
					offs[key] = int64(i)
				}
			} else {
				offs[key] = o
			}
			lastkey = key
		}
		f.Close()
	}

	c, _ := lru.New(65535) // err always nil
	return &Map{
		filename: filename,
		offsets:  offs,
		shiftkey: shift,
		cache:    c,
	}
}

// Get returns a slice of values for the given key.
func (m *Map) Get(key uint64) ([]uint64, bool) {
	if val, ok := m.cache.Get(key); ok {
		v, ok := val.([]uint64)
		return v, ok
	}
	return m.getFromBacking(key)
}

// GetSet returns a set of values for the given key.
func (m *Map) GetSet(key uint64) (map[uint64]struct{}, bool) {
	vals, ok := m.Get(key)
	if !ok {
		return nil, false
	}
	v := make(map[uint64]struct{}, len(vals))
	for _, val := range vals {
		v[val] = struct{}{}
	}
	return v, true
}

///////////////////////////

// Mutate creates a mutable reference to the map. To write any changes to disk,
// you must call Commit first. Mutated keys will not be visible to the parent
// Map until after a reload.
func (m *Map) Mutate() *MutableMap {
	return &MutableMap{
		Map:   m,
		dirty: make(map[uint64][]uint64),
	}
}

// Commit writes the changed entries to disk.
func (m *MutableMap) Commit() error {
	if len(m.dirty) == 0 {
		return nil
	}

	return fmt.Errorf("not implemented")
}

// MutableKey represents a key that is open for writing changes to the set of
// values. Once open, the calling code must call Close to add changes to the
// MutableMap's buffered changes.
type MutableKey struct {
	*MutableMap
	key  uint64
	vals map[uint64]struct{}
}

// OpenKey prepares a key for writing. You must call Close to mark data for
// later commit to disk.
func (m *MutableMap) OpenKey(key uint64) *MutableKey {
	var vals map[uint64]struct{}

	dirtyVals, ok := m.dirty[key]
	if ok {
		vals = make(map[uint64]struct{}, len(dirtyVals))
		for _, v := range dirtyVals {
			vals[v] = struct{}{}
		}
	} else {
		vals, ok := m.Map.GetSet(key)
		if !ok {
			vals = make(map[uint64]struct{}, DefaultCapacity)
		}
	}
	return &MutableKey{
		MutableMap: m,
		key:        key,
		vals:       vals,
	}
}

// Close prepares the key's new data for writing to disk.
func (k *MutableKey) Close() error {
	vals := make([]uint64, 0, len(k.vals))
	for v := range k.vals {
		vals = append(vals, v)
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	k.MutableMap.dirty[k.key] = vals
}

// Clear empties the set of values for the key.
func (k *MutableKey) Clear() {
	for v := range k.vals {
		delete(k.vals, v)
	}
}

// Put adds a value to the key's set.
func (k *MutableKey) Put(val uint64) {
	k.vals[val] = struct{}{}
}

// PutSet adds a set of values to the key's set.
func (k *MutableKey) PutSet(vals map[uint64]struct{}) {
	for val := range vals {
		k.vals[val] = struct{}{}
	}
}

// PutSlice adds a slice of values to the key's set.
func (k *MutableKey) PutSlice(vals []uint64) {
	for _, val := range vals {
		k.vals[val] = struct{}{}
	}
}
