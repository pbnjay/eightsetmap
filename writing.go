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
func Mutate(m Map, autosync bool) *MutableMap {
	sm, ok := m.(*stdMap)
	if !ok {
		log.Println("cannot mutate this map")
		return nil
	}
	return &MutableMap{
		Map:   sm,
		dirty: make(map[uint64][]uint64),

		mutkeys:  make(map[uint64]*MutableKey),
		autosync: autosync,
	}
}

// SetOutputFilename tells this MutableMap to commit to a different filename. Use
// the empty string "" to save to the default filename (the source Map's filename).
func (m *MutableMap) SetOutputFilename(fn string) {
	m.newFilename = fn
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
		if _, ok := m.Map.seekToBackingPosition(key); !ok {
			return false
		}

		var caplen uint64
		err := binary.Read(m.Map.f, binary.LittleEndian, &caplen)
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
	m.Map.f.Close()
	m.Map.f = nil

	f, err := os.OpenFile(m.Map.filename, os.O_RDWR, 0644)
	if err != nil {
		return false
	}
	m.Map.f = f
	defer func() {
		m.Map.f.Close()
		m.Map.f = nil
	}()

	for key, vals := range m.dirty {
		if _, ok := m.Map.seekToBackingPosition(key); !ok {
			return false
		}

		var caplen uint64
		err := binary.Read(m.Map.f, binary.LittleEndian, &caplen)
		if err != nil {
			log.Println(err)
			return false
		}

		c := uint32(caplen >> 32)
		l := uint32(caplen)
		if l != uint32(len(vals)) {
			_, err = m.Map.f.Seek(-8, os.SEEK_CUR)
			if err != nil {
				log.Println(err)
				return false
			}
			caplen = uint64(c)<<32 | uint64(len(vals))
			err := binary.Write(m.Map.f, binary.LittleEndian, caplen)
			if err != nil {
				log.Println(err)
				return false
			}
		}

		err = binary.Write(m.Map.f, binary.LittleEndian, vals)
		if err != nil {
			log.Println(err)
			return false
		}
	}
	// if we got here without failing then all was ok!
	for key, vals := range m.dirty {
		m.Map.cache.Add(key, vals)
		delete(m.dirty, key)
	}
	return true
}

// PackerFunc is a function that tells the serialization code how to pack additional
// data into the file. Additional data MUST by 8-byte aligned, and returned in the
// 'extra' return value. The count must be the number of 8-byte chunks found.
//
// Note that extra must be a data type serializable by `encoding/binary`, but it does
// not necessarily need to be 64bit types.
type PackerFunc func(key uint64, valsize uint32) (count int, extra interface{})

// TightPacker does not reserve any extra space in the disk storage format.
func TightPacker(key uint64, valsize uint32) (count int, extra interface{}) {
	return 0, nil
}

// DefaultPacker tells the serialization code to leave some padding in the file so that
// minimal updates can be performed in-place.
func DefaultPacker(key uint64, valsize uint32) (count int, extra interface{}) {
	// leave extra room to grow
	sz := valsize + (DefaultCapacity - FillFactor)
	sz = DefaultCapacity * (1 + (sz / DefaultCapacity))
	if sz < valsize {
		panic("size mismatch")
	}
	pad := int(sz - valsize)

	return pad, bytes.Repeat([]byte{0}, pad*8)
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
		return m.CommitWithPacker(TightPacker)
	}

	if m.inplaceCommit() {
		return nil
	}

	return m.CommitWithPacker(DefaultPacker)
}

// CommitWithPacker allows the usage of custom data embedded into the lookup table. Maps
// saved using custom packers should not be modified unless you know what you are doing.
//
// Note if autosync is enabled and there are no changes, nothing will be done.
func (m *MutableMap) CommitWithPacker(packer PackerFunc) error {
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

	if m.Map.f != nil {
		m.Map.f.Close()
		m.Map.f = nil
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

	var newf *os.File
	isTemp := true
	if m.newFilename == "" {
		dir, base := filepath.Split(m.Map.filename)
		newf, err = ioutil.TempFile(dir, base)
	} else {
		isTemp = false
		newf, err = os.Create(m.newFilename)
	}
	if err != nil {
		return err
	}
	defer newf.Close()

	// if a shiftkey is in use, we'd have to scan the current file for offsets
	if m.Map.shiftkey > 0 {
		return fmt.Errorf("not implemented")
	}

	x := uint32(MAGIC)
	err = binary.Write(newf, binary.LittleEndian, x)
	if err != nil {
		return err
	}
	// overflow is possible, but if it happens WTF
	x = uint32(len(m.Map.Data))
	err = binary.Write(newf, binary.LittleEndian, x)
	if err != nil {
		return err
	}
	if x != 0 {
		_, err = newf.Write(m.Map.Data)
		if err != nil {
			return err
		}
	}

	/////
	keys := make([]uint64, 0, len(m.Map.offsets)+len(m.dirty))
	for k := range m.Map.offsets {
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

			extraCount, extraData := packer(k, uint32(len(newvals)))
			caplen |= (caplen + uint64(extraCount)) << 32

			err = binary.Write(w, binary.LittleEndian, caplen)
			if err != nil {
				return err
			}
			err = binary.Write(w, binary.LittleEndian, newvals)
			if err != nil {
				return err
			}
			offs += int64(8 + 8*len(newvals) + 8*extraCount)
			if extraCount > 0 {
				err = binary.Write(w, binary.LittleEndian, extraData)
				if err != nil {
					return err
				}
			}

			continue
		}

		_, err = oldf.Seek(m.Map.offsets[k], os.SEEK_SET)
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
		extraCount, extraData := packer(k, uint32(len(vals)))
		caplen |= (caplen + uint64(extraCount)) << 32

		//	copy caplen + values to new file
		err = binary.Write(w, binary.LittleEndian, caplen)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, vals)
		if err != nil {
			return err
		}
		offs += int64(8 + 8*len(vals) + 8*extraCount)
		if extraCount > 0 {
			err = binary.Write(w, binary.LittleEndian, extraData)
			if err != nil {
				return err
			}
		}
	}

	// not used anymore after this
	w.Flush()

	// jump back to the top offset table
	_, err = newf.Seek(int64(16+len(m.Map.Data)), os.SEEK_SET)
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

	if isTemp {
		if oldf != nil {
			err = os.Rename(m.Map.filename, m.Map.filename+".old")
			if err != nil {
				return err
			}
		}

		err = os.Rename(tmpName, m.Map.filename)
		if err != nil {
			start := time.Now()
			var a, b *os.File
			// i get a cross-device link error when i try to move across partitions
			// so let's address that
			a, err = os.Open(tmpName)
			if err == nil {
				b, err = os.Create(m.Map.filename)
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
			err = os.Remove(m.Map.filename + ".old")
			if err != nil {
				log.Println(err)
			}
		}
	}

	// move new data into m.Map so it can be used immediately,
	// and clear out dirty list to be reused...
	m.Map.offsets = newoffsets
	for k, v := range m.dirty {
		m.Map.cache.Add(k, v)
		delete(m.dirty, k)
	}
	m.Map.start = 16 + len(m.Map.Data)
	return nil
}
