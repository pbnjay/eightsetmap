package eightsetmap

import "io"

type Map interface {
	// Get returns a slice of values for the given key.
	Get(key uint64) ([]uint64, bool)

	// GetSet returns a set of values for the given key.
	GetSet(key uint64) (map[uint64]struct{}, bool)

	// GetWithExtra returns a slice of values for the given key, and calls the "extra" func
	// for any additional data stored within the lookup table.
	GetWithExtra(key uint64, extra func(n int, r io.Reader)) ([]uint64, bool)

	// EachKey calls eachFunc for every key in the map until a non-nil error is returned.
	EachKey(eachFunc func(uint64) error) error

	// GetSize gets the size of the set of values for the given key
	GetSize(key uint64) (uint32, bool)

	// GetCapacity gets the capacity reserved for the set of values for the given key
	GetCapacity(key uint64) (uint32, bool)
}
