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

func decodeInt(b []byte) int64 {
	if len(b) == 0 {
		panic("invalid key")
	}

	ret := int64(0)
	shift := 0
	for _, x := range b {
		ret |= int64(x) << shift
		shift += 8
	}
	return ret
}

func decodeVarInt(b []byte) ([]byte, int64) {
	if len(b) == 0 {
		panic("invalid key")
	}

	var x byte
	ret := uint64(0)
	for shift := 0; len(b) != 0 && shift < 64; shift += 7 {
		x, b = b[0], b[1:]
		ret |= uint64(x&0x7f) << shift
		if x&0x80 == 0 {
			return b, int64(ret)
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

	type_byte := a[0]
	a, b = a[1:], b[1:]

	switch type_byte {
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

func (self *keyPrefix) Compare(other *keyPrefix) int {
	if ret := compareInt64(self.DatabaseId, other.DatabaseId); ret != 0 {
		return ret
	}
	if ret := compareInt64(self.ObjectStoreId, other.ObjectStoreId); ret != 0 {
		return ret
	}
	if ret := compareInt64(self.IndexId, other.IndexId); ret != 0 {
		return ret
	}
	return 0
}

func (self *keyPrefix) Type() int {
	if self.DatabaseId == 0 {
		return globalMetadata
	}
	if self.ObjectStoreId == 0 {
		return databaseMetadata
	}
	if self.IndexId == 1 {
		return objectStoreData
	}
	if self.IndexId == 2 {
		return existsEntry
	}
	if self.IndexId == 3 {
		return blobEntry
	}
	if self.IndexId >= 30 {
		return indexData
	}
	return invalidType
}

func decodeKeyPrefix(b []byte) ([]byte, *keyPrefix) {
	if len(b) == 0 {
		panic("invalid key")
	}

	first_byte := b[0]
	b = b[1:]

	database_id_bytes := int((((first_byte >> 5) & 0x07) + 1))
	object_store_id_bytes := int(((first_byte >> 2) & 0x07) + 1)
	index_id_bytes := int((first_byte & 0x03) + 1)

	if len(b) < database_id_bytes+object_store_id_bytes+index_id_bytes {
		panic("invalid key")
	}

	database_id := decodeInt(b[:database_id_bytes])
	b = b[database_id_bytes:]

	object_store_id := decodeInt(b[:object_store_id_bytes])
	b = b[object_store_id_bytes:]

	index_id := decodeInt(b[:index_id_bytes])
	b = b[index_id_bytes:]

	return b, &keyPrefix{DatabaseId: database_id, ObjectStoreId: object_store_id, IndexId: index_id}
}

type indexedDBComparer struct{}

func (indexedDBComparer) Compare(a, b []byte) int {
	defer func(original_a, original_b []byte) {
		if err := recover(); err != nil {
			fmt.Fprintln(os.Stderr, "leveldb: error: idb_cmp1: invalid IndexedDB key found")
			fmt.Fprintf(os.Stderr, "leveldb: debug: a = %x\n", original_a)
			fmt.Fprintf(os.Stderr, "leveldb: debug: b = %x\n", original_b)
		}
	}(a, b)

	a, prefix_a := decodeKeyPrefix(a)
	b, prefix_b := decodeKeyPrefix(b)

	if ret := prefix_a.Compare(prefix_b); ret != 0 {
		return ret
	}

	switch prefix_a.Type() {
	case globalMetadata:
		if len(a) == 0 || len(b) == 0 {
			panic("invalid key")
		}

		if ret := compareByte(a[0], b[0]); ret != 0 {
			return ret
		}

		type_byte := a[0]
		a, b = a[1:], b[1:]

		if type_byte < 7 {
			return 0
		}

		switch type_byte {
		case 50:
			return bytes.Compare(a, b)
		case 100:
			_, database_id_a := decodeVarInt(a)
			_, database_id_b := decodeVarInt(b)
			return compareInt64(database_id_a, database_id_b)
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

		type_byte := a[0]
		a, b = a[1:], b[1:]

		if type_byte < 6 {
			return 0
		}

		switch type_byte {
		case 50:
			a, object_store_id_a := decodeVarInt(a)
			b, object_store_id_b := decodeVarInt(b)
			if ret := compareInt64(object_store_id_a, object_store_id_b); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				panic("invalid key")
			}
			return compareByte(a[0], b[0])
		case 100:
			a, object_store_id_a := decodeVarInt(a)
			b, object_store_id_b := decodeVarInt(b)
			if ret := compareInt64(object_store_id_a, object_store_id_b); ret != 0 {
				return ret
			}

			a, index_id_a := decodeVarInt(a)
			b, index_id_b := decodeVarInt(b)
			if ret := compareInt64(index_id_a, index_id_b); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				panic("invalid key")
			}
			return compareByte(a[0], b[0])
		case 150:
			_, object_store_id_a := decodeVarInt(a)
			_, object_store_id_b := decodeVarInt(b)
			return compareInt64(object_store_id_a, object_store_id_b)
		case 151:
			a, object_store_id_a := decodeVarInt(a)
			b, object_store_id_b := decodeVarInt(b)
			if ret := compareInt64(object_store_id_a, object_store_id_b); ret != 0 {
				return ret
			}

			_, index_id_a := decodeVarInt(a)
			_, index_id_b := decodeVarInt(b)
			return compareInt64(index_id_a, index_id_b)
		case 200:
			_, _, ret := compareStringWithLength(a, b)
			return ret
		case 201:
			a, object_store_id_a := decodeVarInt(a)
			b, object_store_id_b := decodeVarInt(b)
			if ret := compareInt64(object_store_id_a, object_store_id_b); ret != 0 {
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

		var sequence_number_a int64 = -1
		var sequence_number_b int64 = -1
		if len(a) > 0 {
			a, sequence_number_a = decodeVarInt(a)
		}
		if len(b) > 0 {
			b, sequence_number_b = decodeVarInt(b)
		}

		if len(a) == 0 || len(b) == 0 {
			return compareInt(len(a), len(b))
		}

		_, _, ret = compareEncodedIDBKeys(a, b)
		if ret != 0 {
			return ret
		}

		return compareInt64(sequence_number_a, sequence_number_b)
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

var IndexedDBComparer comparer.Comparer = indexedDBComparer{}
