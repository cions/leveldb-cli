// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package indexeddb

import (
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/syndtr/goleveldb/leveldb/util"
)

func succBytes(b []byte) []byte {
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
		return slices.Concat(start, startTail), slices.Concat(start, limitTail)
	}
	if prefix[0] < math.MaxUint8 {
		return slices.Concat(start, startTail), []byte{prefix[0] + 1}
	}
	return slices.Concat(start, startTail), nil
}

func validVarInt(prefix []byte) bool {
	for i := 0; i < len(prefix) && i < 9; i++ {
		if prefix[i]&0x80 == 0 {
			return true
		}
	}
	return false
}

func encodeVarInt(v int64) []byte {
	x := uint64(v)
	buf := make([]byte, 9)
	for i := range len(buf) {
		if x&^0x7f == 0 {
			buf[i] = byte(x)
			n := i + 1
			return buf[:n:n]
		}
		buf[i] = byte(x) | 0x80
		x >>= 7
	}
	panic("encodeVarInt: out of range")
}

func prefixVarInt(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	v := uint64(0)
	minv := uint64(0)
	maxv := uint64(math.MaxInt64)
	i := 0
	for i < len(prefix) {
		v |= uint64(prefix[i]&0x7f) << (7 * i)
		minv = uint64(0x80) << (7 * i)
		maxv &^= uint64(0x7f) << (7 * i)
		if prefix[i]&0x80 == 0 {
			minv = v
			maxv = v
			i++
			break
		}
		if i == 8 {
			return nil, nil
		}
		i++
	}
	minv |= v
	maxv |= v

	var startTail, limitTail []byte
	if len(prefix) > i && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[i:], nexts[1:]...)
	}

	start := encodeVarInt(int64(minv))
	if len(limitTail) > 0 {
		return slices.Concat(start, startTail), slices.Concat(start, limitTail)
	}
	if maxv < math.MaxInt64 {
		return slices.Concat(start, startTail), encodeVarInt(int64(maxv) + 1)
	}
	return slices.Concat(start, startTail), nil
}

func prefixBinary(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	if !validVarInt(prefix) {
		return prefixVarInt(prefix)
	}

	prefix, length := decodeVarInt(prefix)
	body := prefix
	if uint64(len(prefix)) > uint64(length) {
		body = prefix[:length]
	}

	var startTail, limitTail []byte
	if uint64(len(prefix)) > uint64(length) && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[length:], nexts[1:]...)
	}

	start := encodeVarInt(length)
	if len(limitTail) > 0 {
		return slices.Concat(start, body, startTail), slices.Concat(start, body, limitTail)
	}
	if limitBody := succBytes(body); limitBody != nil {
		return slices.Concat(start, body, startTail), slices.Concat(start, limitBody)
	}
	if length < math.MaxInt64 {
		return slices.Concat(start, body, startTail), encodeVarInt(length + 1)
	}
	return slices.Concat(start, body, startTail), nil
}

func prefixStringWithLength(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	if !validVarInt(prefix) {
		return prefixVarInt(prefix)
	}

	prefix, v := decodeVarInt(prefix)
	length := 2 * uint64(v)
	body := prefix
	if uint64(len(prefix)) > length {
		body = prefix[:length]
	}

	var startTail, limitTail []byte
	if uint64(len(prefix)) > length && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[length:], nexts[1:]...)
	}

	start := encodeVarInt(v)
	if len(limitTail) > 0 {
		return slices.Concat(start, body, startTail), slices.Concat(start, body, limitTail)
	}
	if limitBody := succBytes(body); limitBody != nil {
		return slices.Concat(start, body, startTail), slices.Concat(start, limitBody)
	}
	if v < math.MaxInt64 {
		return slices.Concat(start, body, startTail), encodeVarInt(v + 1)
	}
	return slices.Concat(start, body, startTail), nil
}

func prefixDouble(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	if len(prefix) < 8 {
		return nil, nil
	}
	body := prefix[:8]

	var startTail, limitTail []byte
	if len(prefix) > 8 && len(nexts) > 0 {
		startTail, limitTail = nexts[0](prefix[8:], nexts[1:]...)
	}

	if len(limitTail) > 0 {
		return slices.Concat(body, startTail), slices.Concat(body, limitTail)
	}
	return slices.Concat(body, startTail), nil
}

func prefixEncodedIDBKeys(prefix []byte, nexts ...prefixComponent) ([]byte, []byte) {
	var start, limit, startTail, limitTail []byte

	typeByte := prefix[0]
	prefix = prefix[1:]

	switch typeByte {
	case indexedDBKeyNullTypeByte:
		start = []byte{indexedDBKeyNullTypeByte}
		limit = []byte{indexedDBKeyArrayTypeByte}

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	case indexedDBKeyArrayTypeByte:
		start = []byte{indexedDBKeyArrayTypeByte}
		limit = []byte{indexedDBKeyBinaryTypeByte}

		if len(prefix) == 0 {
			break
		}
		if !validVarInt(prefix) {
			startTail, limitTail = prefixVarInt(prefix)
			break
		}
		_, length := decodeVarInt(prefix)

		elements := make([]prefixComponent, length)
		for i := range length {
			elements[i] = prefixEncodedIDBKeys
		}
		nexts = append(elements, nexts...)

		startTail, limitTail = prefixVarInt(prefix, nexts...)
	case indexedDBKeyBinaryTypeByte:
		start = []byte{indexedDBKeyBinaryTypeByte}
		limit = []byte{indexedDBKeyStringTypeByte}

		if len(prefix) > 0 {
			startTail, limitTail = prefixBinary(prefix, nexts...)
		}
	case indexedDBKeyStringTypeByte:
		start = []byte{indexedDBKeyStringTypeByte}
		limit = []byte{indexedDBKeyDateTypeByte}

		if len(prefix) > 0 {
			startTail, limitTail = prefixStringWithLength(prefix, nexts...)
		}
	case indexedDBKeyDateTypeByte:
		start = []byte{indexedDBKeyDateTypeByte}
		limit = []byte{indexedDBKeyNumberTypeByte}

		if len(prefix) > 0 {
			startTail, limitTail = prefixDouble(prefix, nexts...)
		}
	case indexedDBKeyNumberTypeByte:
		start = []byte{indexedDBKeyNumberTypeByte}
		limit = []byte{indexedDBKeyMinKeyTypeByte}

		if len(prefix) > 0 {
			startTail, limitTail = prefixDouble(prefix, nexts...)
		}
	case indexedDBKeyMinKeyTypeByte:
		start = []byte{indexedDBKeyMinKeyTypeByte}
		limit = nil

		if len(prefix) > 0 && len(nexts) > 0 {
			startTail, limitTail = nexts[0](prefix, nexts[1:]...)
		}
	default:
		return nil, nil
	}

	if len(limitTail) > 0 {
		return slices.Concat(start, startTail), slices.Concat(start, limitTail)
	}
	return slices.Concat(start, startTail), limit
}

func prefixKeyBody(prefix []byte, k *keyPrefix) ([]byte, []byte) {
	switch k.Type() {
	case globalMetadata:
		switch prefix[0] {
		case scopesPrefixByte:
			return prefix, succBytes(prefix)
		case databaseFreeListTypeByte:
			return prefixByte(prefix, prefixVarInt)
		case databaseNameTypeByte:
			return prefixByte(prefix, prefixStringWithLength, prefixStringWithLength)
		default:
			return prefixByte(prefix)
		}
	case databaseMetadata:
		switch prefix[0] {
		case objectStoreMetaDataTypeByte:
			return prefixByte(prefix, prefixVarInt, prefixByte)
		case indexMetaDataTypeByte:
			return prefixByte(prefix, prefixVarInt, prefixVarInt, prefixByte)
		case objectStoreFreeListTypeByte:
			return prefixByte(prefix, prefixVarInt)
		case indexFreeListTypeByte:
			return prefixByte(prefix, prefixVarInt, prefixVarInt)
		case objectStoreNamesTypeByte:
			return prefixByte(prefix, prefixStringWithLength)
		case indexNamesKeyTypeByte:
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
	default:
		return nil, nil
	}
}

func encodeKeyPrefix(k *keyPrefix) []byte {
	if k == nil {
		return nil
	}

	databaseIdBytes := max((bits.Len64(uint64(k.DatabaseId))+7)/8, 1)
	objectStoreIdBytes := max((bits.Len64(uint64(k.ObjectStoreId))+7)/8, 1)
	indexIdBytes := max((bits.Len32(uint32(k.IndexId))+7)/8, 1)

	encoded := make([]byte, 1, 1+databaseIdBytes+objectStoreIdBytes+indexIdBytes)
	encoded[0] = byte(((databaseIdBytes - 1) << 5) | ((objectStoreIdBytes - 1) << 2) | (indexIdBytes - 1))

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(k.DatabaseId))
	encoded = append(encoded, buf[:databaseIdBytes]...)
	binary.LittleEndian.PutUint64(buf[:], uint64(k.ObjectStoreId))
	encoded = append(encoded, buf[:objectStoreIdBytes]...)
	binary.LittleEndian.PutUint32(buf[:], uint32(k.IndexId))
	encoded = append(encoded, buf[:indexIdBytes]...)

	return encoded
}

func succKeyPrefix(k *keyPrefix) *keyPrefix {
	succ := &keyPrefix{
		DatabaseId:    k.DatabaseId,
		ObjectStoreId: k.ObjectStoreId,
		IndexId:       k.IndexId,
	}

	if succ.IndexId < math.MaxUint32 {
		succ.IndexId++
		return succ
	}
	succ.IndexId = 0

	if succ.ObjectStoreId < math.MaxInt64 {
		succ.ObjectStoreId++
		return succ
	}
	succ.ObjectStoreId = 0

	if succ.DatabaseId < math.MaxInt64 {
		succ.DatabaseId++
		return succ
	}
	return nil
}

func prefixPartialKeyPrefix(prefix []byte) ([]byte, []byte) {
	databaseIdBytes := int((((prefix[0] >> 5) & 0x07) + 1))
	objectStoreIdBytes := int(((prefix[0] >> 2) & 0x07) + 1)
	indexIdBytes := int((prefix[0] & 0x03) + 1)

	prefix = prefix[1:]
	mink := &keyPrefix{}
	maxk := &keyPrefix{
		DatabaseId:    math.MaxInt64,
		ObjectStoreId: math.MaxInt64,
		IndexId:       math.MaxUint32,
	}

	if len(prefix) >= databaseIdBytes {
		mink.DatabaseId = decodeInt(prefix[:databaseIdBytes])
		maxk.DatabaseId = decodeInt(prefix[:databaseIdBytes])
		prefix = prefix[databaseIdBytes:]
	} else {
		if databaseIdBytes > 1 {
			mink.DatabaseId = int64(1) << (8 * (databaseIdBytes - 1))
		}
		if databaseIdBytes < 8 {
			maxk.DatabaseId = (int64(1) << (8 * databaseIdBytes)) - 1
		}
		if len(prefix) > 0 {
			v := decodeInt(prefix)
			mink.DatabaseId |= v
			maxk.DatabaseId &^= (int64(1) << (8 * len(prefix))) - 1
			maxk.DatabaseId |= v
			prefix = prefix[len(prefix):]
		}
	}

	if len(prefix) >= objectStoreIdBytes {
		mink.ObjectStoreId = decodeInt(prefix[:objectStoreIdBytes])
		maxk.ObjectStoreId = decodeInt(prefix[:objectStoreIdBytes])
		prefix = prefix[objectStoreIdBytes:]
	} else {
		if objectStoreIdBytes > 1 {
			mink.ObjectStoreId = int64(1) << (8 * (objectStoreIdBytes - 1))
		}
		if objectStoreIdBytes < 8 {
			maxk.ObjectStoreId = (int64(1) << (8 * objectStoreIdBytes)) - 1
		}
		if len(prefix) > 0 {
			v := decodeInt(prefix)
			mink.ObjectStoreId |= v
			maxk.ObjectStoreId &^= (int64(1) << (8 * len(prefix))) - 1
			maxk.ObjectStoreId |= v
			prefix = prefix[len(prefix):]
		}
	}

	if indexIdBytes > 1 {
		mink.IndexId = int64(1) << (8 * (indexIdBytes - 1))
	}
	maxk.IndexId = (int64(1) << (8 * indexIdBytes)) - 1
	if len(prefix) > 0 {
		v := decodeInt(prefix)
		mink.IndexId |= v
		maxk.IndexId &^= (int64(1) << (8 * len(prefix))) - 1
		maxk.IndexId |= v
	}

	return encodeKeyPrefix(mink), encodeKeyPrefix(succKeyPrefix(maxk))
}

func prefixKeyPrefix(prefix []byte) ([]byte, []byte) {
	databaseIdBytes := int((((prefix[0] >> 5) & 0x07) + 1))
	objectStoreIdBytes := int(((prefix[0] >> 2) & 0x07) + 1)
	indexIdBytes := int((prefix[0] & 0x03) + 1)

	if len(prefix) < 1+databaseIdBytes+objectStoreIdBytes+indexIdBytes {
		return prefixPartialKeyPrefix(prefix)
	}

	prefix, keyPrefix := decodeKeyPrefix(prefix)

	var startTail, limitTail []byte
	if len(prefix) > 0 {
		startTail, limitTail = prefixKeyBody(prefix, keyPrefix)
	}

	start := encodeKeyPrefix(keyPrefix)
	if len(limitTail) > 0 {
		return slices.Concat(start, startTail), slices.Concat(start, limitTail)
	}
	return slices.Concat(start, startTail), encodeKeyPrefix(succKeyPrefix(keyPrefix))
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
