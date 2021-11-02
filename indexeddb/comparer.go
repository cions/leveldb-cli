package indexeddb

import (
	"bytes"
	"fmt"
	"os"
	"unsafe"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

// Reference: https://source.chromium.org/chromium/chromium/src/+/main:content/browser/indexed_db/indexed_db_leveldb_coding.cc
//            https://chromium.googlesource.com/chromium/src/+/main/content/browser/indexed_db/docs/leveldb_coding_scheme.md

const (
	globalMetadata = iota
	databaseMetadata
	objectStoreData
	existsEntry
	indexData
	invalidType
	blobEntry
)

func decodeInt(slice []byte) int64 {
	if len(slice) == 0 {
		panic("invalid key")
	}

	v := int64(0)
	shift := 0
	for _, b := range slice {
		v |= int64(b) << shift
		shift += 8
		if shift >= 64 {
			panic("invalid key")
		}
	}
	return v
}

func decodeVarInt(slice []byte) ([]byte, int64) {
	if len(slice) == 0 {
		panic("invalid key")
	}

	v := uint64(0)
	for shift := 0; len(slice) != 0 && shift < 64; shift += 7 {
		b := slice[0]
		slice = slice[1:]
		v |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return slice, int64(v)
		}
	}
	panic("invalid key")
}

func compareByte(a, b byte) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareInt64(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func compareBinary(a, b []byte) ([]byte, []byte, int) {
	a, len1 := decodeVarInt(a)
	if len1 < 0 || int64(len(a)) < len1 {
		panic("invalid key")
	}

	b, len2 := decodeVarInt(b)
	if len2 < 0 || int64(len(b)) < len2 {
		panic("invalid key")
	}

	return a[len1:], b[len2:], bytes.Compare(a[:len1], b[:len2])
}

func compareStringWithLength(a, b []byte) ([]byte, []byte, int) {
	a, len1 := decodeVarInt(a)
	if len1 < 0 || int64(len(a)) < 2*len1 {
		panic("invalid key")
	}

	b, len2 := decodeVarInt(b)
	if len2 < 0 || int64(len(b)) < 2*len2 {
		panic("invalid key")
	}

	return a[2*len1:], b[2*len2:], bytes.Compare(a[:2*len1], b[:2*len2])
}

func compareDouble(a, b []byte) ([]byte, []byte, int) {
	if len(a) < 8 || len(b) < 8 {
		panic("invalid key")
	}

	f1 := *(*float64)(unsafe.Pointer(&a[0]))
	f2 := *(*float64)(unsafe.Pointer(&b[0]))

	if f1 < f2 {
		return a[8:], b[8:], -1
	}
	if f1 > f2 {
		return a[8:], b[8:], 1
	}
	return a[8:], b[8:], 0
}

func keyTypeByteToKeyType(b byte) int {
	switch b {
	case 0:
		return 0
	case 4:
		return 1
	case 6:
		return 2
	case 1:
		return 3
	case 2:
		return 4
	case 3:
		return 5
	case 5:
		return 7
	}
	return 0
}

func compareEncodedIDBKeys(a, b []byte) ([]byte, []byte, int) {
	if len(a) == 0 || len(b) == 0 {
		panic("invalid key")
	}

	if ret := compareInt(keyTypeByteToKeyType(a[0]), keyTypeByteToKeyType(b[0])); ret != 0 {
		return a[1:], b[1:], ret
	}

	typeByte := a[0]
	a, b = a[1:], b[1:]

	switch typeByte {
	case 0, 5:
		return a, b, 0
	case 4:
		var ret int
		a, len1 := decodeVarInt(a)
		b, len2 := decodeVarInt(b)
		for i := int64(0); i < len1 && i < len2; i++ {
			a, b, ret = compareEncodedIDBKeys(a, b)
			if ret != 0 {
				return a, b, ret
			}
		}
		return a, b, compareInt64(len1, len2)
	case 6:
		return compareBinary(a, b)
	case 1:
		return compareStringWithLength(a, b)
	case 2, 3:
		return compareDouble(a, b)
	}
	panic("invalid key")
}

type keyPrefix struct {
	DatabaseId, ObjectStoreId, IndexId int64
}

func (prefix *keyPrefix) Type() int {
	if prefix.DatabaseId == 0 {
		return globalMetadata
	}
	if prefix.ObjectStoreId == 0 {
		return databaseMetadata
	}
	if prefix.IndexId == 1 {
		return objectStoreData
	}
	if prefix.IndexId == 2 {
		return existsEntry
	}
	if prefix.IndexId == 3 {
		return blobEntry
	}
	if prefix.IndexId >= 30 {
		return indexData
	}
	return invalidType
}

func decodeKeyPrefix(b []byte) ([]byte, *keyPrefix) {
	if len(b) == 0 {
		panic("invalid key")
	}

	firstByte := b[0]
	b = b[1:]

	databaseIdBytes := int((((firstByte >> 5) & 0x07) + 1))
	objectStoreIdBytes := int(((firstByte >> 2) & 0x07) + 1)
	indexIdBytes := int((firstByte & 0x03) + 1)

	if len(b) < databaseIdBytes+objectStoreIdBytes+indexIdBytes {
		panic("invalid key")
	}

	databaseId := decodeInt(b[:databaseIdBytes])
	b = b[databaseIdBytes:]

	objectStoreId := decodeInt(b[:objectStoreIdBytes])
	b = b[objectStoreIdBytes:]

	indexId := decodeInt(b[:indexIdBytes])
	b = b[indexIdBytes:]

	return b, &keyPrefix{DatabaseId: databaseId, ObjectStoreId: objectStoreId, IndexId: indexId}
}

func compareKeyPrefix(a, b *keyPrefix) int {
	if ret := compareInt64(a.DatabaseId, b.DatabaseId); ret != 0 {
		return ret
	}
	if ret := compareInt64(a.ObjectStoreId, b.ObjectStoreId); ret != 0 {
		return ret
	}
	if ret := compareInt64(a.IndexId, b.IndexId); ret != 0 {
		return ret
	}
	return 0
}

type indexedDBComparer struct{}

func (indexedDBComparer) Compare(a, b []byte) int {
	defer func(a, b []byte) {
		if err := recover(); err != nil {
			fmt.Fprintln(os.Stderr, "leveldb: error: idb_cmp1: invalid IndexedDB key found")
			fmt.Fprintf(os.Stderr, "leveldb: debug: a = %x\n", a)
			fmt.Fprintf(os.Stderr, "leveldb: debug: b = %x\n", b)
		}
	}(a, b)

	a, prefixA := decodeKeyPrefix(a)
	b, prefixB := decodeKeyPrefix(b)

	if ret := compareKeyPrefix(prefixA, prefixB); ret != 0 {
		return ret
	}

	switch prefixA.Type() {
	case globalMetadata:
		if len(a) == 0 || len(b) == 0 {
			panic("invalid key")
		}

		if ret := compareByte(a[0], b[0]); ret != 0 {
			return ret
		}

		typeByte := a[0]
		a, b = a[1:], b[1:]

		if typeByte < 7 {
			return 0
		}

		switch typeByte {
		case 50:
			return bytes.Compare(a, b)
		case 100:
			_, databaseIdA := decodeVarInt(a)
			_, databaseIdB := decodeVarInt(b)
			return compareInt64(databaseIdA, databaseIdB)
		case 201:
			a, b, ret := compareStringWithLength(a, b)
			if ret != 0 {
				return ret
			}

			_, _, ret = compareStringWithLength(a, b)
			return ret
		}
	case databaseMetadata:
		if len(a) == 0 || len(b) == 0 {
			panic("invalid key")
		}

		if ret := compareByte(a[0], b[0]); ret != 0 {
			return ret
		}

		typeByte := a[0]
		a, b = a[1:], b[1:]

		if typeByte < 6 {
			return 0
		}

		switch typeByte {
		case 50:
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := compareInt64(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				panic("invalid key")
			}
			return compareByte(a[0], b[0])
		case 100:
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := compareInt64(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			a, indexIdA := decodeVarInt(a)
			b, indexIdB := decodeVarInt(b)
			if ret := compareInt64(indexIdA, indexIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				panic("invalid key")
			}
			return compareByte(a[0], b[0])
		case 150:
			_, objectStoreIdA := decodeVarInt(a)
			_, objectStoreIdB := decodeVarInt(b)
			return compareInt64(objectStoreIdA, objectStoreIdB)
		case 151:
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := compareInt64(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			_, indexIdA := decodeVarInt(a)
			_, indexIdB := decodeVarInt(b)
			return compareInt64(indexIdA, indexIdB)
		case 200:
			_, _, ret := compareStringWithLength(a, b)
			return ret
		case 201:
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := compareInt64(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			_, _, ret := compareStringWithLength(a, b)
			return ret
		}
	case objectStoreData:
		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case existsEntry:
		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case blobEntry:
		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case indexData:
		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		a, b, ret := compareEncodedIDBKeys(a, b)
		if ret != 0 {
			return ret
		}

		var sequenceNumberA int64 = -1
		var sequenceNumberB int64 = -1
		if len(a) > 0 {
			a, sequenceNumberA = decodeVarInt(a)
		}
		if len(b) > 0 {
			b, sequenceNumberB = decodeVarInt(b)
		}

		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		_, _, ret = compareEncodedIDBKeys(a, b)
		if ret != 0 {
			return ret
		}

		return compareInt64(sequenceNumberA, sequenceNumberB)
	}
	panic("invalid key")
}

func (indexedDBComparer) Name() string {
	return "idb_cmp1"
}

func (indexedDBComparer) Separator(dst, a, b []byte) []byte {
	return nil
}

func (indexedDBComparer) Successor(dst, b []byte) []byte {
	return nil
}

// IndexedDBComparer implements the idb_cmp1 comparer used in Chromium IndexedDB implementation.
var IndexedDBComparer comparer.Comparer = indexedDBComparer{}
