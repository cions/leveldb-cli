package indexeddb

import (
	"encoding/binary"
	"math"
	"math/bits"

	"github.com/syndtr/goleveldb/leveldb/util"
)

func succBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < math.MaxUint8 {
			return append(b[:i:i], b[i]+1)
		}
	}
	return nil
}

type prefixComponent func(prefix []byte, nexts ...prefixComponent) ([]byte, []byte)

func prefixByte(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	var startTail, limitTail []byte
	if len(prefix) > 1 && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[1:], nexts[1:]...)
	}

	start := []byte{prefix[0]}
	if len(limitTail) > 0 {
		return append(start, startTail...), append(start, limitTail...)
	}
	if prefix[0] == math.MaxUint8 {
		return append(start, startTail...), nil
	}
	return append(start, startTail...), []byte{prefix[0] + 1}
}

func validVarInt(prefix []byte) bool {
	for i := 0; i < len(prefix) && i < 9; i++ {
		if prefix[i]&0x80 == 0 {
			return true
		}
	}
	return false
}

func prefixVarInt(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	var v uint64 = 0
	var min uint64 = 0
	var max uint64 = math.MaxInt64
	i := 0
	for i < len(prefix) {
		v |= uint64(prefix[i]&0x7f) << (7 * i)
		min = uint64(0x80) << (7 * i)
		max &^= uint64(0x7f) << (7 * i)
		if prefix[i]&0x80 == 0 {
			min = v
			max = v
			i++
			break
		}
		if i == 8 {
			return nil, nil
		}
		i++
	}
	min |= v
	max |= v

	var startTail, limitTail []byte
	if len(prefix) > i && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[i:], nexts[1:]...)
	}

	start := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(start, min)
	start = start[:n:n]
	if len(limitTail) > 0 {
		return append(start, startTail...), append(start, limitTail...)
	}
	if max == math.MaxInt64 {
		return append(start, startTail...), nil
	}
	limit := make([]byte, binary.MaxVarintLen64)
	m := binary.PutUvarint(limit, max+1)
	return append(start, startTail...), limit[:m]
}

func prefixBinary(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	if !validVarInt(prefix) {
		return prefixVarInt(prefix)
	}
	startLength, limitLength := prefixVarInt(prefix)
	prefix, length := decodeVarInt(prefix)
	body := prefix
	if uint64(len(body)) > uint64(length) {
		body = body[:length]
	}

	var startTail, limitTail []byte
	if uint64(len(prefix)) > uint64(length) && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[length:], nexts[1:]...)
	}

	if len(limitTail) > 0 {
		start := append(startLength, body...)
		start = start[:len(start):len(start)]
		return append(start, startTail...), append(start, limitTail...)
	}
	limitBody := succBytes(body)
	if limitBody == nil {
		return append(append(startLength, body...), startTail...), limitLength
	}
	return append(append(startLength, body...), startTail...), append(append([]byte{}, startLength...), limitBody...)
}

func prefixStringWithLength(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	if !validVarInt(prefix) {
		return prefixVarInt(prefix)
	}
	startLength, limitLength := prefixVarInt(prefix)
	prefix, v := decodeVarInt(prefix)
	length := 2 * uint64(v)
	body := prefix
	if uint64(len(body)) > length {
		body = body[:length]
	}

	var startTail, limitTail []byte
	if uint64(len(prefix)) > length && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[length:], nexts[1:]...)
	}

	if len(limitTail) > 0 {
		start := append(startLength, body...)
		start = start[:len(start):len(start)]
		return append(start, startTail...), append(start, limitTail...)
	}
	limitBody := succBytes(body)
	if limitBody == nil {
		return append(append(startLength, body...), startTail...), limitLength
	}
	return append(append(startLength, body...), startTail...), append(append([]byte{}, startLength...), limitBody...)
}

func prefixEncodedIDBKeys(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	var start, limit []byte
	var startTail, limitTail []byte

	typeByte := prefix[0]
	prefix = prefix[1:]

	switch typeByte {
	case 0:
		start = []byte{0}
		limit = []byte{4}

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	case 4:
		start = []byte{4}
		limit = []byte{6}

		if len(prefix) == 0 {
			break
		}
		if !validVarInt(prefix) {
			startTail, limitTail = prefixVarInt(prefix)
			break
		}
		_, length := decodeVarInt(prefix)

		elements := make([]prefixComponent, length)
		for i := int64(0); i < length; i++ {
			elements[i] = prefixEncodedIDBKeys
		}
		nexts = append(elements, nexts...)

		startTail, limitTail = prefixVarInt(prefix, nexts...)
	case 6:
		start = []byte{6}
		limit = []byte{1}

		if len(prefix) > 0 {
			startTail, limitTail = prefixBinary(prefix, nexts...)
		}
	case 1:
		start = []byte{1}
		limit = []byte{2}

		if len(prefix) > 0 {
			startTail, limitTail = prefixStringWithLength(prefix, nexts...)
		}
	case 2:
		start = []byte{2}
		limit = []byte{3}

		if len(prefix) < 8 {
			break
		}
		start = append(start, prefix[:8]...)
		prefix = prefix[8:]

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	case 3:
		start = []byte{3}
		limit = []byte{5}

		if len(prefix) < 8 {
			break
		}
		start = append(start, prefix[:8]...)
		prefix = prefix[8:]

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	case 5:
		start = []byte{5}
		limit = nil

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	default:
		return nil, nil
	}

	if len(limitTail) > 0 {
		return append(start, startTail...), append(start, limitTail...)
	}
	return append(start, startTail...), limit
}

func prefixKeyRest(prefix []byte, k *keyPrefix) ([]byte, []byte) {
	switch k.Type() {
	case globalMetadata:
		switch prefix[0] {
		case 50:
			return prefix, succBytes(prefix)
		case 100:
			return prefixByte(prefix, prefixVarInt)
		case 201:
			return prefixByte(prefix, prefixStringWithLength, prefixStringWithLength)
		default:
			return prefixByte(prefix)
		}
	case databaseMetadata:
		switch prefix[0] {
		case 50:
			return prefixByte(prefix, prefixVarInt, prefixByte)
		case 100:
			return prefixByte(prefix, prefixVarInt, prefixVarInt, prefixByte)
		case 150:
			return prefixByte(prefix, prefixVarInt)
		case 151:
			return prefixByte(prefix, prefixVarInt, prefixVarInt)
		case 200:
			return prefixByte(prefix, prefixStringWithLength)
		case 201:
			return prefixByte(prefix, prefixVarInt, prefixStringWithLength)
		default:
			return prefixByte(prefix)
		}
	case objectStoreData:
		return prefixEncodedIDBKeys(prefix)
	case existsEntry:
		return prefixEncodedIDBKeys(prefix)
	case blobEntry:
		return prefixEncodedIDBKeys(prefix)
	case indexData:
		return prefixEncodedIDBKeys(prefix)
	}
	return nil, nil
}

func encodeKeyPrefix(k *keyPrefix) []byte {
	databaseIdBytes := 1
	if k.DatabaseId != 0 {
		databaseIdBytes = (bits.Len64(uint64(k.DatabaseId)) + 7) / 8
	}
	objectStoreIdBytes := 1
	if k.ObjectStoreId != 0 {
		objectStoreIdBytes = (bits.Len64(uint64(k.ObjectStoreId)) + 7) / 8
	}
	indexIdBytes := 1
	if k.IndexId != 0 {
		indexIdBytes = (bits.Len32(uint32(k.IndexId)) + 7) / 8
	}

	encoded := make([]byte, 1+databaseIdBytes+objectStoreIdBytes+indexIdBytes)
	encoded[0] = byte(((databaseIdBytes - 1) << 5) | ((objectStoreIdBytes - 1) << 2) | (indexIdBytes - 1))

	var buf [8]byte
	startidx := 1
	binary.LittleEndian.PutUint64(buf[:], uint64(k.DatabaseId))
	startidx += copy(encoded[startidx:], buf[:databaseIdBytes])
	binary.LittleEndian.PutUint64(buf[:], uint64(k.ObjectStoreId))
	startidx += copy(encoded[startidx:], buf[:objectStoreIdBytes])
	binary.LittleEndian.PutUint64(buf[:], uint64(k.IndexId))
	copy(encoded[startidx:], buf[:indexIdBytes])

	return encoded
}

func succKeyPrefix(k *keyPrefix) (*keyPrefix, bool) {
	succ := &keyPrefix{
		DatabaseId:    k.DatabaseId,
		ObjectStoreId: k.ObjectStoreId,
		IndexId:       k.IndexId,
	}

	if succ.IndexId < math.MaxUint32 {
		succ.IndexId += 1
		return succ, false
	}
	succ.IndexId = 0

	if succ.ObjectStoreId < math.MaxInt64 {
		succ.ObjectStoreId += 1
		return succ, false
	}
	succ.ObjectStoreId = 0

	if succ.DatabaseId < math.MaxInt64 {
		succ.DatabaseId += 1
		return succ, false
	}
	return nil, true
}

func prefixKeyPrefix(prefix []byte) ([]byte, []byte) {
	databaseIdBytes := int((((prefix[0] >> 5) & 0x07) + 1))
	objectStoreIdBytes := int(((prefix[0] >> 2) & 0x07) + 1)
	indexIdBytes := int((prefix[0] & 0x03) + 1)

	if len(prefix) < 1+databaseIdBytes+objectStoreIdBytes+indexIdBytes {
		prefix := prefix[1:]
		start := &keyPrefix{}
		last := &keyPrefix{
			DatabaseId:    math.MaxInt64,
			ObjectStoreId: math.MaxInt64,
			IndexId:       math.MaxUint32,
		}

		if len(prefix) >= databaseIdBytes {
			start.DatabaseId = decodeInt(prefix[:databaseIdBytes])
			last.DatabaseId = decodeInt(prefix[:databaseIdBytes])
			prefix = prefix[databaseIdBytes:]
		} else {
			if databaseIdBytes > 1 {
				start.DatabaseId = int64(1) << (8 * (databaseIdBytes - 1))
			}
			if databaseIdBytes < 8 {
				last.DatabaseId = (int64(1) << (8 * databaseIdBytes)) - 1
			}
			if len(prefix) > 0 {
				v := decodeInt(prefix)
				start.DatabaseId |= v
				last.DatabaseId &^= (int64(1) << (8 * len(prefix))) - 1
				last.DatabaseId |= v
				prefix = prefix[len(prefix):]
			}
		}

		if len(prefix) >= objectStoreIdBytes {
			start.ObjectStoreId = decodeInt(prefix[:objectStoreIdBytes])
			last.ObjectStoreId = decodeInt(prefix[:objectStoreIdBytes])
			prefix = prefix[objectStoreIdBytes:]
		} else {
			if objectStoreIdBytes > 1 {
				start.ObjectStoreId = int64(1) << (8 * (objectStoreIdBytes - 1))
			}
			if objectStoreIdBytes < 8 {
				last.ObjectStoreId = (int64(1) << (8 * objectStoreIdBytes)) - 1
			}
			if len(prefix) > 0 {
				v := decodeInt(prefix)
				start.ObjectStoreId |= v
				last.ObjectStoreId &^= (int64(1) << (8 * len(prefix))) - 1
				last.ObjectStoreId |= v
				prefix = prefix[len(prefix):]
			}
		}

		if indexIdBytes > 1 {
			start.IndexId = int64(1) << (8 * (indexIdBytes - 1))
		}
		last.IndexId = (int64(1) << (8 * indexIdBytes)) - 1
		if len(prefix) > 0 {
			v := decodeInt(prefix)
			start.IndexId |= v
			last.IndexId &^= (int64(1) << (8 * len(prefix))) - 1
			last.IndexId |= v
		}

		limit, max := succKeyPrefix(last)
		if max {
			return encodeKeyPrefix(start), nil
		}
		return encodeKeyPrefix(start), encodeKeyPrefix(limit)
	}

	prefix, keyPrefix := decodeKeyPrefix(prefix)

	var startTail, limitTail []byte
	if len(prefix) > 0 {
		startTail, limitTail = prefixKeyRest(prefix, keyPrefix)
	}

	start := encodeKeyPrefix(keyPrefix)
	if len(limitTail) > 0 {
		return append(start, startTail...), append(start, limitTail...)
	}
	limitKeyPrefix, max := succKeyPrefix(keyPrefix)
	if max {
		return append(start, startTail...), nil
	}
	return append(start, startTail...), encodeKeyPrefix(limitKeyPrefix)
}

// Prefix returns a key range that satisfy the given prefix for the idb_cmp1 comparer.
func Prefix(prefix []byte) *util.Range {
	if len(prefix) == 0 {
		return nil
	}

	start, limit := prefixKeyPrefix(prefix)
	if len(limit) == 0 {
		return &util.Range{Start: start}
	}
	return &util.Range{Start: start, Limit: limit}
}
