package eightsetmap

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"unsafe"

	"github.com/tysontate/gommap"
)

// memMap represents a mmap'd view of the on-disk Map format.
type memMap struct {
	// the raw data
	mmap gommap.MMap

	// map to unsafe slices backed by the mmap above
	nodes  map[uint64][]uint64
	extras map[uint64][]byte
}

// unsafely cast a byte array to a uint64 array
func touint64(x []byte) []uint64 {
	xx := make([]uint64, 0, 0)
	hdrp := (*reflect.SliceHeader)(unsafe.Pointer(&xx))
	hdrp.Data = (*reflect.SliceHeader)(unsafe.Pointer(&x)).Data
	hdrp.Len = len(x) / 8
	hdrp.Cap = len(x) / 8
	return xx
}

func MMap(mp Map) Map {
	m, ok := mp.(*stdMap)
	if !ok {
		panic("cannot mmap this type of map")
	}
	f, err := os.Open(m.filename)
	if err != nil {
		panic(err)
	}
	info, err := f.Stat()
	if err != nil {
		panic(err)
	}

	x, err := gommap.MapRegion(f.Fd(), 0, info.Size(), gommap.PROT_READ, gommap.MAP_SHARED)
	if err != nil {
		panic(err)
	}

	mm := &memMap{
		mmap:   x,
		nodes:  make(map[uint64][]uint64),
		extras: make(map[uint64][]byte),
	}

	for k, offs := range m.offsets {
		caplen := binary.LittleEndian.Uint64(x[offs : offs+8])
		offs += 8
		offend1 := offs + int64(uint32(caplen)*8)
		offend2 := offs + int64(uint32(caplen>>32)*8)

		p1 := x[offs:offend1]
		mm.nodes[k] = touint64(p1)

		if offend1 != offend2 {
			mm.extras[k] = x[offend1:offend2]
		}
	}

	return mm
}

// Get returns a slice of values for the given key.
func (m *memMap) Get(key uint64) ([]uint64, bool) {
	val, ok := m.nodes[key]
	return val, ok
}

// GetSet returns a set of values for the given key.
func (m *memMap) GetSet(key uint64) (map[uint64]struct{}, bool) {
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
func (m *memMap) GetWithExtra(key uint64, extra func(n int, r io.Reader)) ([]uint64, bool) {
	if b, ok := m.extras[key]; ok {
		buf := bytes.NewBuffer(b)
		extra(len(b)/8, buf)
	}
	val, ok := m.nodes[key]
	return val, ok
}

// EachKey calls eachFunc for every key in the map until a non-nil error is returned.
func (m *memMap) EachKey(eachFunc func(uint64) error) error {
	for k := range m.nodes {
		err := eachFunc(k)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSize gets the size of the set of values for the given key
func (m *memMap) GetSize(key uint64) (uint32, bool) {
	val, ok := m.nodes[key]
	return uint32(len(val)), ok
}

// GetCapacity gets the capacity reserved for the set of values for the given key
func (m *memMap) GetCapacity(key uint64) (uint32, bool) {
	val, ok := m.nodes[key]
	v2, _ := m.extras[key]
	return uint32(len(val) + (len(v2) / 8)), ok
}
