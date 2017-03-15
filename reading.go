package eightsetmap

import (
	"encoding/binary"
	"log"
	"os"
)

// seekToBackingPosition moves to the position in the backing file for the key
func (m *stdMap) seekToBackingPosition(key uint64) (int64, bool) {
	var err error
	if m.f == nil {
		m.f, err = os.Open(m.filename)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Println(err)
			}
			return 0, false
		}
	}

	offs, ok := m.offsets[key>>m.shiftkey]
	if !ok {
		return 0, false
	}

	if m.shiftkey > 0 {
		// jump to the lookup table and find the true offset
		_, err = m.f.Seek(int64(m.start)+(offs*16), os.SEEK_SET)
		if err != nil {
			return 0, false
		}

		var okey uint64
		if key == 0 {
			okey = 1
		}
		for okey != key {
			// uint64 key
			err = binary.Read(m.f, binary.LittleEndian, &okey)
			if err != nil {
				panic(err)
			}

			if (okey >> m.shiftkey) != (key >> m.shiftkey) {
				// key not found
				return 0, false
			}

			// int64 offset
			err = binary.Read(m.f, binary.LittleEndian, &offs)
			if err != nil {
				panic(err)
			}
		}

		if offs == 0 {
			// should not happen, but just in case...
			return 0, false
		}
	}

	offs, err = m.f.Seek(offs, os.SEEK_SET)
	return offs, err == nil
}

// getFromBacking gets the set of values from the backing file
func (m *stdMap) getFromBacking(key uint64) ([]uint64, bool) {
	_, ok := m.seekToBackingPosition(key)
	if !ok {
		return nil, false
	}

	// 64bit int, upper 32bits capacity, lower 32bits length
	var caplen uint64
	err := binary.Read(m.f, binary.LittleEndian, &caplen)
	if err != nil {
		log.Println(err)
		return nil, false
	}

	// downcast to get just length
	l := uint32(caplen)
	if l == 0 {
		return []uint64{}, true
	}
	vals := make([]uint64, l)

	// NB m.f is open/valid due to seekToBackingPosition
	err = binary.Read(m.f, binary.LittleEndian, &vals)
	if err != nil {
		log.Println(err)
		return nil, false
	}

	m.cache.Add(key, vals)
	return vals, true
}

// GetSize gets the size of the set of values for the given key
func (m *stdMap) GetSize(key uint64) (uint32, bool) {
	_, ok := m.seekToBackingPosition(key)
	if !ok {
		return 0, false
	}

	var caplen uint64
	err := binary.Read(m.f, binary.LittleEndian, &caplen)
	if err != nil {
		log.Println(err)
		return 0, false
	}

	// downcast to get just length
	return uint32(caplen), true
}

// GetCapacity gets the capacity reserved for the set of values for the given key
func (m *stdMap) GetCapacity(key uint64) (uint32, bool) {
	_, ok := m.seekToBackingPosition(key)
	if !ok {
		return 0, false
	}

	var caplen uint64
	err := binary.Read(m.f, binary.LittleEndian, &caplen)
	if err != nil {
		log.Println(err)
		return 0, false
	}

	// shift+downcast to get just capacity
	return uint32(caplen >> 32), true
}
