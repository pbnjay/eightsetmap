// Package eightsetmap contains a hyper-specialized map[uint64][]uint64 wrapper
// that implements an out-of-core storage method. So if you have a machine with
// 16G of RAM, you can still access 64G of data.
package eightsetmap

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/golang-lru"
)

const (
	// Magic header uint32 = 'j8sm' defines the file type.
	MAGIC uint32 = 0x6d73386a
)

var (
	// DefaultCacheSize is the number of keys to keep in a LRU cache for each map.
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
type stdMap struct {
	filename string
	f        *os.File // readonly file
	start    int      // lookup table start offset

	// 1 billion keys here will easily take over 16gb...
	offsets  map[uint64]int64
	shiftkey uint64

	//cache map[uint64][]uint64
	cache *lru.Cache

	// Data contains the custom data embedded within the on-disk format.
	Data []byte
}

// MutableMap represents a Map that can be written to.
type MutableMap struct {
	Map         *stdMap
	newFilename string

	// not yet committed to disk
	dirty   map[uint64][]uint64
	mutkeys map[uint64]*MutableKey

	// should keys be auto-synced?
	autosync bool
}

// New returns a new Map backed by the (possibly empty) data in filename.
func New(filename string) Map {
	return NewShifted(filename, 0)
}

// NewShifted returns a Map with shifting enabled to reduce core memory usage.
// A shift is a power of 2 factor, so shift=1 means that memory usage is
// approximately cut in half, but that lookups will take additional disk seeks.
func NewShifted(filename string, shift uint64) Map {
	var cdata []byte
	offs := make(map[uint64]int64)
	f, err := os.Open(filename)
	if err == nil {
		var x uint32
		err = binary.Read(f, binary.LittleEndian, &x)
		if err != nil {
			panic(err)
		}
		if x != MAGIC {
			panic("this is not an 8sm file (magic invalid)")
		}
		// read in size of custom data section
		err = binary.Read(f, binary.LittleEndian, &x)
		if err != nil {
			panic(err)
		}
		if x > 0 {
			cdata = make([]byte, x)
			n, err := f.Read(cdata)
			if err != nil {
				panic(err)
			}
			if n != int(x) {
				panic("did not fully read custom data")
			}
		}

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
	return &stdMap{
		filename: filename,
		start:    16 + len(cdata),
		offsets:  offs,
		shiftkey: shift,
		cache:    c,

		Data: cdata,
	}
}

// Get returns a slice of values for the given key.
func (m *stdMap) Get(key uint64) ([]uint64, bool) {
	if val, ok := m.cache.Get(key); ok {
		v, ok := val.([]uint64)
		return v, ok
	}
	return m.getFromBacking(key)
}

// GetSet returns a set of values for the given key.
func (m *stdMap) GetSet(key uint64) (map[uint64]struct{}, bool) {
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

// GetWithExtra returns a slice of values for the given key, and calls the "extra" func
// for any additional data stored within the lookup table.
func (m *stdMap) GetWithExtra(key uint64, extra func(n int, r io.Reader)) ([]uint64, bool) {
	return m.getWithExtraFromBacking(key, extra)
}

// EachKey calls eachFunc for every key in the map until a non-nil error is returned.
func (m *stdMap) EachKey(eachFunc func(uint64) error) error {
	if m.shiftkey > 0 {
		return fmt.Errorf("not yet implemented")
	}
	for k := range m.offsets {
		err := eachFunc(k)
		if err != nil {
			return err
		}
	}
	return nil
}
