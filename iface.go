package eightsetmap

type Map interface {
	// Get returns a slice of values for the given key.
	Get(key uint64) ([]uint64, bool)

	// GetSet returns a set of values for the given key.
	GetSet(key uint64) (map[uint64]struct{}, bool)

	// EachKey calls eachFunc for every key in the map until a non-nil error is returned.
	EachKey(eachFunc func(uint64) error) error

	// GetSize gets the size of the set of values for the given key
	GetSize(key uint64) (uint32, bool)

	// GetCapacity gets the capacity reserved for the set of values for the given key
	GetCapacity(key uint64) (uint32, bool)
}
