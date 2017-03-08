package eightsetmap

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
)

var (
	// DefaultCapacity of sets within the backing file
	DefaultCapacity uint32 = 32

	// FillFactor is the fill cutoff to bump to the next capacity size when saving.
	// e.g. if FillFactor out of DefaultCapacity slots are used in the last bucket,
	// add more capacity.
	FillFactor uint32 = 24
)

// Mutate creates a mutable reference to the map. To write any changes to disk,
// you must call Commit first. Mutated keys will not be visible to the parent
// Map until after a reload.
func (m *Map) Mutate() *MutableMap {
	return &MutableMap{
		Map:   m,
		dirty: make(map[uint64][]uint64),
	}
}

// Get returns a slice of values for the given key. If there is a newly
// written, uncommitted key then it will be returned.
func (m *MutableMap) Get(key uint64) ([]uint64, bool) {
	if vals, ok := m.dirty[key]; ok {
		return vals, true
	}
	return m.Map.Get(key)
}

// GetSet returns a set of values for the given key. If there is a newly
// written, uncommitted key then it will be returned.
func (m *MutableMap) GetSet(key uint64) (map[uint64]struct{}, bool) {
	if vals, ok := m.dirty[key]; ok {
		mv := make(map[uint64]struct{})
		for _, v := range vals {
			mv[v] = struct{}{}
		}
		return mv, true
	}
	return m.Map.GetSet(key)
}

// MutableKey represents a key that is open for writing changes to the set of
// values. Once open, the calling code must call Sync to add changes to the
// MutableMap's buffered changes.
type MutableKey struct {
	*MutableMap
	key  uint64
	vals map[uint64]struct{}
}

// OpenKey prepares a key for writing. You must call Sync to mark data for
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
		vals, ok = m.Map.GetSet(key)
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

// Sync prepares the key's new data for writing to disk by copying updates to the
// linked MutableMap. MutableKey may continue to be used but will not reflect other
// changes outside of it own scope (since OpenKey).
func (k *MutableKey) Sync() {
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

// Commit writes the changed entries to disk. If packed is true, then no empty room is left
// for later expansion. The MutableMap can be immediately reused after a successful commit.
func (m *MutableMap) Commit(packed bool) error {
	if len(m.dirty) == 0 {
		// nothing to write!
		return nil
	}

	if m.f != nil {
		m.f.Close()
		m.f = nil
	}

	oldf, err := os.Open(m.Map.filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}
	if err == nil {
		defer oldf.Close()
	}

	dir, base := filepath.Split(m.Map.filename)
	newf, err := ioutil.TempFile(dir, base)
	if err != nil {
		return err
	}
	defer newf.Close()

	// if a shiftkey is in use, we have to scan the current file to copy values
	// cause it's too big to fit into memory
	if m.shiftkey > 0 {
		return fmt.Errorf("not implemented")
		//return m.indirectCommit(oldf, newf, packed)
	}

	/////
	keys := make([]uint64, 0, len(m.offsets)+len(m.dirty))
	for k := range m.offsets {
		keys = append(keys, k)
	}
	for k := range m.dirty {
		if _, ok := m.Map.offsets[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	totalKeys := uint64(len(keys))
	err = binary.Write(newf, binary.LittleEndian, totalKeys)
	if err != nil {
		return err
	}

	newoffsets := make(map[uint64]int64, len(keys))

	// start writing keys and offsets
	for _, k := range keys {
		// write out the key
		err = binary.Write(newf, binary.LittleEndian, k)
		if err != nil {
			return err
		}
		// placeholder for offset
		err = binary.Write(newf, binary.LittleEndian, k)
		if err != nil {
			return err
		}

		newoffsets[k] = 0
	}

	////////
	padding := make([]uint64, 2*DefaultCapacity)
	for _, k := range keys {
		newoffsets[k], err = newf.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		var caplen uint64
		if newvals, ok := m.dirty[k]; ok {
			caplen = uint64(len(newvals))
			var pad uint32

			if !packed {
				// leave extra room to grow
				sz := uint32(len(newvals)) + (DefaultCapacity - FillFactor)
				sz = DefaultCapacity * (1 + (sz / DefaultCapacity))
				if sz < uint32(len(newvals)) {
					panic("size mismatch")
				}
				pad = sz - uint32(len(newvals))

				caplen |= uint64(sz) << 32
			} else {
				caplen |= caplen << 32
			}
			err = binary.Write(newf, binary.LittleEndian, caplen)
			if err != nil {
				return err
			}
			err = binary.Write(newf, binary.LittleEndian, newvals)
			if err != nil {
				return err
			}
			if pad > 0 {
				err = binary.Write(newf, binary.LittleEndian, padding[:pad])
				if err != nil {
					return err
				}
			}

			continue
		}

		_, err = oldf.Seek(m.offsets[k], os.SEEK_SET)
		if err != nil {
			return err
		}

		err = binary.Read(oldf, binary.LittleEndian, &caplen)
		if err != nil {
			return err
		}

		// shift+downcast to get capacity
		vals := make([]uint64, uint32(caplen>>32))
		err = binary.Read(oldf, binary.LittleEndian, &vals)
		if err != nil {
			return err
		}

		if packed {
			// repack if necessary
			caplen = uint64(uint32(caplen)) | (caplen << 32)
			vals = vals[:uint32(caplen)]
		}

		//	copy caplen + values to new file
		err = binary.Write(newf, binary.LittleEndian, caplen)
		if err != nil {
			return err
		}
		err = binary.Write(newf, binary.LittleEndian, vals)
		if err != nil {
			return err
		}
	}

	// jump back to the top offset table
	_, err = newf.Seek(8, os.SEEK_SET)
	if err != nil {
		return err
	}

	// now write correct offsets
	for _, k := range keys {
		// write out the key (again)
		err = binary.Write(newf, binary.LittleEndian, k)
		if err != nil {
			return err
		}
		// now write the real offset
		err = binary.Write(newf, binary.LittleEndian, newoffsets[k])
		if err != nil {
			return err
		}
	}

	////////

	tmpName := newf.Name()
	err = newf.Close()
	if err != nil {
		return err
	}

	if oldf != nil {
		err = os.Rename(m.filename, m.filename+".old")
		if err != nil {
			return err
		}
	}

	err = os.Rename(tmpName, m.filename)
	if err != nil {
		return err
	}
	if oldf != nil {
		err = os.Remove(m.filename + ".old")
		if err != nil {
			log.Println(err)
		}
	}

	// move new data into m.Map so it can be used immediately,
	// and clear out dirty list to be reused...
	m.offsets = newoffsets
	for k, v := range m.dirty {
		m.cache.Add(k, v)
		delete(m.dirty, k)
	}
	return nil
}
