# eightsetmap
a hyper-specialized data structure that supports a map from 64-bit integers onto
sorted sets of 64-bit integers without being constrained to available memory.

It's like a `map[uint64]map[uint64]struct{}` but the maps are sorted, or
like a `map[uint64][]uint64` but the slice has no duplicates.

The data is stored on disk in a way that makes direct access reasonably
efficient, and allows for standard set operations like union, intersection,
and difference. Manipulation of the data is supported, but the data structure
and operations are optimized primarly for heavy read access patterns, and
essentially minimal writes. Essentially write once, then access a lot.

Simply keeping the metadata for 1 billion 64bit keys (and matching 64bit offsets
to their sets) would take over 16Gb of memory (and that doesn't even
include the sets themselves). So this framework uses a **shift** value to
truncate keys for an intermediate lookup. This is a simple bitwise shift right
on the key, so each increment cuts the key storage requirements approximately
in half. When using a shift>0, the storage layer will do an initial lookup to
see if a key in the right range is even present, and then do a fast lookup (at
most reading 2^shift total key-offsets) on the disk for the actual key.
