package eightsetmap

import (
	"encoding/binary"
	"os"
	"sort"
)

var (
	DefaultCapacity uint32 = 32
)

func (m *Map) persistKey(key uint64) error {
	return nil
}

func (m *Map) openFile() error {
	if m.f != nil {
		m.f.Close()
	}

	f, err := os.OpenFile(m.filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	m.wf = f
	m.f = f

	return nil
}

func (m *Map) putToBacking(key uint64, vals map[uint64]struct{}) error {
	if m.wf == nil {
		err := m.openFile()
		if err != nil {
			return err
		}
	}

	v := make([]uint64, 0, len(vals))
	for val := range vals {
		v = append(v, val)
	}
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })

	offs, ok := m.seekToBackingPosition(key)
	if !ok {
		// FIXME:
	}

	////////////

	// 32bit capacity, 32bit length, packed into 64bits
	var caplen uint64
	err := binary.Read(m.f, binary.LittleEndian, &caplen)
	if err != nil {
		return err
	}

	cap := uint32(caplen >> 32)
	if cap < uint32(len(v)) {
		// move the the end and make a new one
	}

	err = binary.Write(m.f, binary.LittleEndian, v)
	if err != nil {
		return err
	}

	return nil
}

// Checkpoint quickly saves data to disk without being terribly efficient.
// Until this command returns data is not recoverable.
func (m *Map) Checkpoint() error {
	return nil
}

// Repack recovers wasted space in the current data file by repacking it into
// a new file. If withReserve is false, then no additional capacity is reserved
// and additions will be more expensive.
func (m *Map) Repack(tempfile string, withReserve bool) error {
	if err := m.Checkpoint(); err != nil {
		return err
	}
	keys := make([]uint64, 0, len(m.offsets)+len(m.newoffsets))
	for k := range m.offsets {
		keys = append(keys, k)
	}
	for k := range m.newoffsets {
		keys = append(keys, k)
	}

	return nil
}
