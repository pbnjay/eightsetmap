package eightsetmap

import (
	"encoding/binary"
	"log"
	"os"
)

func (m *Map) getFromCache(key uint64) ([]uint64, bool) {
	islice, ok := m.cache.Get(key)
	if !ok {
		return nil, false
	}
	vv, ok := islice.([]uint64)
	return vv, ok
}

// seekToBackingPosition moves the the position in the backing file for the key
func (m *Map) seekToBackingPosition(key uint64) (int64, bool) {
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
		_, err = m.f.Seek(8+(offs*16), os.SEEK_SET)
		if err != nil {
			return 0, false
		}

		var okey uint64
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

func (m *Map) getFromBacking(key uint64) ([]uint64, bool) {
	offs, ok := m.seekToBackingPosition(key)
	if !ok {
		return nil, false
	}

	// 32bit capacity, 32bit length, packed into 64bits
	var caplen uint64
	err := binary.Read(m.f, binary.LittleEndian, &caplen)
	if err != nil {
		log.Println(err)
		return nil, false
	}

	// upper 32bits = capacity, dropped to get just length
	vals := make([]uint64, uint32(caplen))
	err = binary.Read(m.f, binary.LittleEndian, &vals)
	if err != nil {
		log.Println(err)
		return nil, false
	}

	m.cache.Add(key, vals)
	return vals, true
}
