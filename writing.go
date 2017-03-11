package eightsetmap

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
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
//
// If autosync is true, then mutated keys are automatically Sync()ed when
// Commit is called. If false, then you must Sync() mutated keys manually to
// pull them into a Commit.
func (m *Map) Mutate(autosync bool) *MutableMap {
	return &MutableMap{
		Map:   m,
		dirty: make(map[uint64][]uint64),

		mutkeys:  make(map[uint64]*MutableKey),
		autosync: autosync,
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

	synced bool
}

// OpenKey prepares a key for writing. You must call Sync to mark data for
// later commit to disk.
func (m *MutableMap) OpenKey(key uint64) *MutableKey {
	if mk, ok := m.mutkeys[key]; ok {
		return mk
	}
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
	mk := &MutableKey{
		MutableMap: m,
		key:        key,
		vals:       vals,
		synced:     true,
	}
	m.mutkeys[key] = mk
	return mk
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
	k.synced = true
}

// Discard frees up internal references to this key to release memory.
func (k *MutableKey) Discard() {
	for v := range k.vals {
		delete(k.vals, v)
	}
	delete(k.MutableMap.mutkeys, k.key)
	k.MutableMap = nil
	k.vals = nil
}

// Clear empties the set of values for the key.
func (k *MutableKey) Clear() {
	for v := range k.vals {
		delete(k.vals, v)
		k.synced = false
	}
}

// Put adds a value to the key's set.
func (k *MutableKey) Put(val uint64) {
	k.vals[val] = struct{}{}
	k.synced = false
}

// PutSet adds a set of values to the key's set.
func (k *MutableKey) PutSet(vals map[uint64]struct{}) {
	for val := range vals {
		k.vals[val] = struct{}{}
		k.synced = false
	}
}

// PutSlice adds a slice of values to the key's set.
func (k *MutableKey) PutSlice(vals []uint64) {
	for _, val := range vals {
		k.vals[val] = struct{}{}
		k.synced = false
	}
}

// Remove a value from the key's set.
func (k *MutableKey) Remove(val uint64) {
	delete(k.vals, val)
	k.synced = false
}

// RemoveSet removes a set of values from the key's set.
func (k *MutableKey) RemoveSet(vals map[uint64]struct{}) {
	for val := range vals {
		delete(k.vals, val)
		k.synced = false
	}
}

// RemoveSlice removes a slice of values from the key's set.
func (k *MutableKey) RemoveSlice(vals []uint64) {
	for _, val := range vals {
		delete(k.vals, val)
		k.synced = false
	}
}

// inplaceCommit tries to put new values into the map without rewriting the
// whole file. It returns true on success.
func (m *MutableMap) inplaceCommit() bool {
	for key, vals := range m.dirty {
		if _, ok := m.seekToBackingPosition(key); !ok {
			return false
		}

		var caplen uint64
		err := binary.Read(m.f, binary.LittleEndian, &caplen)
		if err != nil {
			log.Println(err)
			return false
		}

		c := uint32(caplen >> 32)
		if c < uint32(len(vals)) {
			// will not fit without resize
			return false
		}
	}
	// passed checks, we can update in-place!
	m.f.Close()
	m.f = nil

	f, err := os.OpenFile(m.filename, os.O_RDWR, 0644)
	if err != nil {
		return false
	}
	m.f = f
	defer func() {
		m.f.Close()
		m.f = nil
	}()

	for key, vals := range m.dirty {
		if _, ok := m.seekToBackingPosition(key); !ok {
			return false
		}

		var caplen uint64
		err := binary.Read(m.f, binary.LittleEndian, &caplen)
		if err != nil {
			log.Println(err)
			return false
		}

		c := uint32(caplen >> 32)
		l := uint32(caplen)
		if l != uint32(len(vals)) {
			_, err = m.f.Seek(-8, os.SEEK_CUR)
			if err != nil {
				log.Println(err)
				return false
			}
			caplen = uint64(c)<<32 | uint64(len(vals))
			err := binary.Write(m.f, binary.LittleEndian, caplen)
			if err != nil {
				log.Println(err)
				return false
			}
		}

		err = binary.Write(m.f, binary.LittleEndian, vals)
		if err != nil {
			log.Println(err)
			return false
		}
	}
	// if we got here without failing then all was ok!
	for key, vals := range m.dirty {
		m.cache.Add(key, vals)
		delete(m.dirty, key)
	}
	return true
}

// PackerFunc is a function that tells the serialization code how to pack additional
// space into the file. It takes the number of values in the set (valsize) and returns
// the 'capacity' value (capval) to encode into the key's header, along with the number
// of padding bytes to leave after the set data (extraBytes).
//
// To maintain data alignment, it is recommended to make extraBytes a multiple of 8.
type PackerFunc func(valsize uint32) (capval uint32, extraBytes uint32)

// ExtraFunc is a function that provides additional data for a key, which will be
// placed directly after the key's set of values.
type ExtraFunc func(key uint64) []byte

// TightPacker does not reserve any extra space in the disk storage format.
func TightPacker(valsize uint32) (capval uint32, pad uint32) {
	return valsize, 0
}

// DefaultPacker tells the serialization code to leave some padding in the file so that
// minimal updates can be performed in-place.
func DefaultPacker(valsize uint32) (capval uint32, pad uint32) {
	// leave extra room to grow
	sz := valsize + (DefaultCapacity - FillFactor)
	sz = DefaultCapacity * (1 + (sz / DefaultCapacity))
	if sz < valsize {
		panic("size mismatch")
	}
	pad = sz - valsize

	return sz, pad * 8
}

// Commit writes the changed entries to disk. If packed is true, then no empty room is left
// for later expansion. The MutableMap can be immediately reused after a successful commit.
//
// If packed is false, then a much faster in-place commit is possible (using the additional
// space reserved from the previous un-packed commit). If an in-place commit is not possible
// then a standard full commit will be used.
//
// Note if autosync is enabled and there are no changes, nothing will be done.
func (m *MutableMap) Commit(packed bool) error {
	if m.autosync {
		for k, mk := range m.mutkeys {
			if !mk.synced {
				mk.Sync()
			}
			delete(m.mutkeys, k)
		}
		if len(m.dirty) == 0 {
			// nothing to write!
			return nil
		}
	}

	if packed {
		return m.CommitWithPacker(TightPacker, nil)
	}

	if m.inplaceCommit() {
		return nil
	}

	return m.CommitWithPacker(DefaultPacker, nil)
}

// CommitWithPacker allows the usage of custom data embedded into the lookup table. Maps
// saved using custom packers should not be modified unless you know what you are doing.
//
// Note if autosync is enabled and there are no changes, nothing will be done.
func (m *MutableMap) CommitWithPacker(packer PackerFunc, extra ExtraFunc) error {
	if m.autosync {
		for k, mk := range m.mutkeys {
			if !mk.synced {
				mk.Sync()
			}
			delete(m.mutkeys, k)
		}

		if len(m.dirty) == 0 {
			// nothing to write!
			return nil
		}
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

	x := uint32(MAGIC)
	err = binary.Write(newf, binary.LittleEndian, x)
	if err != nil {
		return err
	}
	// overflow is possible, but if it happens WTF
	x = uint32(len(m.Data))
	err = binary.Write(newf, binary.LittleEndian, x)
	if err != nil {
		return err
	}
	if x != 0 {
		_, err = newf.Write(m.Data)
		if err != nil {
			return err
		}
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

	offs, err := newf.Seek(0, os.SEEK_CUR)
	w := bufio.NewWriterSize(newf, 50000000) //50mb buffer

	////////
	for _, k := range keys {
		newoffsets[k] = offs

		var caplen uint64
		if newvals, ok := m.dirty[k]; ok {
			caplen = uint64(len(newvals))

			sz, pad := packer(uint32(len(newvals)))
			caplen |= uint64(sz) << 32

			err = binary.Write(w, binary.LittleEndian, caplen)
			if err != nil {
				return err
			}
			err = binary.Write(w, binary.LittleEndian, newvals)
			if err != nil {
				return err
			}
			offs += int64(8 + 8*len(newvals))
			if pad > 0 && extra != nil {
				edata := extra(k)
				if len(edata) > 0 {
					_, err = w.Write(edata)
					if err != nil {
						return err
					}
					offs += int64(len(edata))
					pad -= uint32(len(edata))
				}
			}
			if pad > 0 {
				offs += int64(pad)
				_, err = w.Write(bytes.Repeat([]byte{0}, int(pad)))
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

		// truncate to values only
		vals = vals[:uint32(caplen)]
		caplen = uint64(len(vals))
		sz, pad := packer(uint32(len(vals)))
		caplen |= uint64(sz) << 32

		//	copy caplen + values to new file
		err = binary.Write(w, binary.LittleEndian, caplen)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, vals)
		if err != nil {
			return err
		}
		offs += int64(8 + 8*len(vals))
		if pad > 0 && extra != nil {
			edata := extra(k)
			if len(edata) > 0 {
				_, err = w.Write(edata)
				if err != nil {
					return err
				}
				offs += int64(len(edata))
				pad -= uint32(len(edata))
			}
		}
		if pad > 0 {
			offs += int64(pad)
			_, err = w.Write(bytes.Repeat([]byte{0}, int(pad)))
			if err != nil {
				return err
			}
		}
	}

	// not used anymore after this
	w.Flush()

	// jump back to the top offset table
	_, err = newf.Seek(int64(16+len(m.Data)), os.SEEK_SET)
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
		start := time.Now()
		var a, b *os.File
		// i get a cross-device link error when i try to move across partitions
		// so let's address that
		a, err = os.Open(tmpName)
		if err == nil {
			b, err = os.Create(m.filename)
			if err == nil {
				_, err = io.Copy(b, a)
				b.Close()
			}
			a.Close()
		}
		elap := time.Now().Sub(start)
		log.Println("took", elap, "to copy across partitions")
	}
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
	m.start = 16 + len(m.Data)
	return nil
}
