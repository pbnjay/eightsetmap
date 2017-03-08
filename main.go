package eightsetmap

import (
	"encoding/binary"
	"os"

	"github.com/hashicorp/golang-lru"
)

var (
	DefaultCacheSize = 65535
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
// approximately cut in half, but that lookups will take additional disk seeks.
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

	c, _ := lru.New(DefaultCacheSize) // err always nil
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
