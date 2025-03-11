// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package indexeddb

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/syndtr/goleveldb/leveldb/comparer"
)

// References:
//   https://source.chromium.org/chromium/chromium/src/+/main:content/browser/indexed_db/indexed_db_leveldb_coding.cc
//   https://chromium.googlesource.com/chromium/src/+/main/content/browser/indexed_db/docs/leveldb_coding_scheme.md

const (
	globalMetadata   = 0
	databaseMetadata = 1
	objectStoreData  = 2
	existsEntry      = 3
	indexData        = 4
	invalidType      = 5
	blobEntry        = 6
)

const (
	objectStoreDataIndexId = 1
	existsEntryIndexId     = 2
	blobEntryIndexId       = 3
	minimumIndexId         = 30
)

const (
	maxSimpleGlobalMetaDataTypeByte = 7
	scopesPrefixByte                = 50
	databaseFreeListTypeByte        = 100
	databaseNameTypeByte            = 201
)

const (
	maxSimpleDatabaseMetaDataTypeByte = 6
	objectStoreMetaDataTypeByte       = 50
	indexMetaDataTypeByte             = 100
	objectStoreFreeListTypeByte       = 150
	indexFreeListTypeByte             = 151
	objectStoreNamesTypeByte          = 200
	indexNamesKeyTypeByte             = 201
)

const (
	indexedDBKeyNullTypeByte   = 0
	indexedDBKeyStringTypeByte = 1
	indexedDBKeyDateTypeByte   = 2
	indexedDBKeyNumberTypeByte = 3
	indexedDBKeyArrayTypeByte  = 4
	indexedDBKeyMinKeyTypeByte = 5
	indexedDBKeyBinaryTypeByte = 6
)

const (
	indexedDBInvalidKeyType = 0
	indexedDBArrayKeyType   = 1
	indexedDBBinaryKeyType  = 2
	indexedDBStringKeyType  = 3
	indexedDBDateKeyType    = 4
	indexedDBNumberKeyType  = 5
	indexedDBNoneKeyType    = 6
	indexedDBMinKeyType     = 7
)

func decodeInt(a []byte) int64 {
	if len(a) == 0 || len(a) > 8 {
		panic("invalid key")
	}
	v := uint64(0)
	for i, b := range a {
		v |= uint64(b) << (8 * i)
	}
	return int64(v)
}

func decodeVarInt(a []byte) ([]byte, int64) {
	v := uint64(0)
	for i := 0; i < len(a) && i < 9; i++ {
		v |= uint64(a[i]&0x7f) << (7 * i)
		if a[i]&0x80 == 0 {
			return a[i+1:], int64(v)
		}
	}
	panic("invalid key")
}

func compareBinary(a, b []byte) ([]byte, []byte, int) {
	a, len1 := decodeVarInt(a)
	b, len2 := decodeVarInt(b)

	if uint64(len(a)) < uint64(len1) || uint64(len(b)) < uint64(len2) {
		minlen := min(uint64(len1), uint64(len2), uint64(len(a)), uint64(len(b)))
		if ret := bytes.Compare(a[:minlen], b[:minlen]); ret != 0 {
			return nil, nil, ret
		}
		return nil, nil, cmp.Compare(len1, len2)
	}

	return a[len1:], b[len2:], bytes.Compare(a[:len1], b[:len2])
}

func compareStringWithLength(a, b []byte) ([]byte, []byte, int) {
	a, v1 := decodeVarInt(a)
	len1 := 2 * uint64(v1)
	b, v2 := decodeVarInt(b)
	len2 := 2 * uint64(v2)

	if uint64(len(a)) < len1 || uint64(len(b)) < len2 {
		minlen := min(len1, len2, uint64(len(a)), uint64(len(b)))
		if ret := bytes.Compare(a[:minlen], b[:minlen]); ret != 0 {
			return nil, nil, ret
		}
		return nil, nil, cmp.Compare(v1, v2)
	}

	return a[len1:], b[len2:], bytes.Compare(a[:len1], b[:len2])
}

func compareDouble(a, b []byte) ([]byte, []byte, int) {
	if len(a) < 8 || len(b) < 8 {
		panic("invalid key")
	}

	f1 := math.Float64frombits(binary.NativeEndian.Uint64(a))
	f2 := math.Float64frombits(binary.NativeEndian.Uint64(b))
	return a[8:], b[8:], cmp.Compare(f1, f2)
}

func keyTypeByteToKeyType(b byte) int {
	switch b {
	case indexedDBKeyNullTypeByte:
		return indexedDBInvalidKeyType
	case indexedDBKeyArrayTypeByte:
		return indexedDBArrayKeyType
	case indexedDBKeyBinaryTypeByte:
		return indexedDBBinaryKeyType
	case indexedDBKeyStringTypeByte:
		return indexedDBStringKeyType
	case indexedDBKeyDateTypeByte:
		return indexedDBDateKeyType
	case indexedDBKeyNumberTypeByte:
		return indexedDBNumberKeyType
	case indexedDBKeyMinKeyTypeByte:
		return indexedDBMinKeyType
	default:
		return indexedDBInvalidKeyType
	}
}

func compareEncodedIDBKeys(a, b []byte) ([]byte, []byte, int) {
	if len(a) == 0 || len(b) == 0 {
		return a, b, cmp.Compare(len(a), len(b))
	}

	ret := cmp.Compare(keyTypeByteToKeyType(a[0]), keyTypeByteToKeyType(b[0]))
	if ret != 0 {
		return a[1:], b[1:], ret
	}

	typeByte := a[0]
	a, b = a[1:], b[1:]

	switch typeByte {
	case indexedDBKeyNullTypeByte, indexedDBKeyMinKeyTypeByte:
		return a, b, 0
	case indexedDBKeyArrayTypeByte:
		if len(a) == 0 || len(b) == 0 {
			return a, b, cmp.Compare(len(a), len(b))
		}
		a, len1 := decodeVarInt(a)
		b, len2 := decodeVarInt(b)
		for range min(len1, len2) {
			if len(a) == 0 || len(b) == 0 {
				break
			}
			a, b, ret = compareEncodedIDBKeys(a, b)
			if ret != 0 {
				return a, b, ret
			}
		}
		return a, b, cmp.Compare(len1, len2)
	case indexedDBKeyBinaryTypeByte:
		if len(a) == 0 || len(b) == 0 {
			return a, b, cmp.Compare(len(a), len(b))
		}
		return compareBinary(a, b)
	case indexedDBKeyStringTypeByte:
		if len(a) == 0 || len(b) == 0 {
			return a, b, cmp.Compare(len(a), len(b))
		}
		return compareStringWithLength(a, b)
	case indexedDBKeyDateTypeByte, indexedDBKeyNumberTypeByte:
		if len(a) == 0 || len(b) == 0 {
			return a, b, cmp.Compare(len(a), len(b))
		}
		return compareDouble(a, b)
	default:
		panic("invalid key")
	}
}

type keyPrefix struct {
	DatabaseId, ObjectStoreId, IndexId int64
}

func (prefix *keyPrefix) Type() int {
	switch {
	case prefix.DatabaseId == 0:
		return globalMetadata
	case prefix.ObjectStoreId == 0:
		return databaseMetadata
	case prefix.IndexId == objectStoreDataIndexId:
		return objectStoreData
	case prefix.IndexId == existsEntryIndexId:
		return existsEntry
	case prefix.IndexId == blobEntryIndexId:
		return blobEntry
	case prefix.IndexId >= minimumIndexId:
		return indexData
	default:
		return invalidType
	}
}

func decodeKeyPrefix(a []byte) ([]byte, *keyPrefix) {
	if len(a) == 0 {
		panic("invalid key")
	}

	firstByte := a[0]
	a = a[1:]

	databaseIdBytes := int((((firstByte >> 5) & 0x07) + 1))
	objectStoreIdBytes := int(((firstByte >> 2) & 0x07) + 1)
	indexIdBytes := int((firstByte & 0x03) + 1)

	if len(a) < databaseIdBytes+objectStoreIdBytes+indexIdBytes {
		panic("invalid key")
	}

	databaseId := decodeInt(a[:databaseIdBytes])
	a = a[databaseIdBytes:]

	objectStoreId := decodeInt(a[:objectStoreIdBytes])
	a = a[objectStoreIdBytes:]

	indexId := decodeInt(a[:indexIdBytes])
	a = a[indexIdBytes:]

	return a, &keyPrefix{databaseId, objectStoreId, indexId}
}

func compareKeyPrefix(a, b *keyPrefix) int {
	if ret := cmp.Compare(a.DatabaseId, b.DatabaseId); ret != 0 {
		return ret
	}
	if ret := cmp.Compare(a.ObjectStoreId, b.ObjectStoreId); ret != 0 {
		return ret
	}
	if ret := cmp.Compare(a.IndexId, b.IndexId); ret != 0 {
		return ret
	}
	return 0
}

type idbCmp1 struct{}

func (idbCmp1) Compare(a, b []byte) int {
	defer func(a, b []byte) {
		if err := recover(); err != nil {
			fmt.Fprintln(os.Stderr, "leveldb: warning: idb_cmp1: invalid IndexedDB key found")
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
			return cmp.Compare(len(a), len(b))
		}
		if ret := cmp.Compare(a[0], b[0]); ret != 0 {
			return ret
		}

		typeByte := a[0]
		a, b = a[1:], b[1:]

		if typeByte < maxSimpleGlobalMetaDataTypeByte {
			return 0
		}

		switch typeByte {
		case scopesPrefixByte:
			return bytes.Compare(a, b)
		case databaseFreeListTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, databaseIdA := decodeVarInt(a)
			_, databaseIdB := decodeVarInt(b)
			return cmp.Compare(databaseIdA, databaseIdB)
		case databaseNameTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, b, ret := compareStringWithLength(a, b)
			if ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, _, ret = compareStringWithLength(a, b)
			return ret
		default:
			panic("invalid key")
		}
	case databaseMetadata:
		if len(a) == 0 || len(b) == 0 {
			return cmp.Compare(len(a), len(b))
		}
		if ret := cmp.Compare(a[0], b[0]); ret != 0 {
			return ret
		}

		typeByte := a[0]
		a, b = a[1:], b[1:]

		if typeByte < maxSimpleDatabaseMetaDataTypeByte {
			return 0
		}

		switch typeByte {
		case objectStoreMetaDataTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := cmp.Compare(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			return cmp.Compare(a[0], b[0])
		case indexMetaDataTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := cmp.Compare(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, indexIdA := decodeVarInt(a)
			b, indexIdB := decodeVarInt(b)
			if ret := cmp.Compare(indexIdA, indexIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			return cmp.Compare(a[0], b[0])
		case objectStoreFreeListTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, objectStoreIdA := decodeVarInt(a)
			_, objectStoreIdB := decodeVarInt(b)
			return cmp.Compare(objectStoreIdA, objectStoreIdB)
		case indexFreeListTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := cmp.Compare(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, indexIdA := decodeVarInt(a)
			_, indexIdB := decodeVarInt(b)
			return cmp.Compare(indexIdA, indexIdB)
		case objectStoreNamesTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, _, ret := compareStringWithLength(a, b)
			return ret
		case indexNamesKeyTypeByte:
			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			a, objectStoreIdA := decodeVarInt(a)
			b, objectStoreIdB := decodeVarInt(b)
			if ret := cmp.Compare(objectStoreIdA, objectStoreIdB); ret != 0 {
				return ret
			}

			if len(a) == 0 || len(b) == 0 {
				return cmp.Compare(len(a), len(b))
			}
			_, _, ret := compareStringWithLength(a, b)
			return ret
		default:
			panic("invalid key")
		}
	case objectStoreData:
		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case existsEntry:
		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case blobEntry:
		_, _, ret := compareEncodedIDBKeys(a, b)
		return ret
	case indexData:
		a, b, ret := compareEncodedIDBKeys(a, b)
		if ret != 0 {
			return ret
		}

		sequenceNumberA := int64(-1)
		sequenceNumberB := int64(-1)
		if len(a) > 0 {
			a, sequenceNumberA = decodeVarInt(a)
		}
		if len(b) > 0 {
			b, sequenceNumberB = decodeVarInt(b)
		}

		if len(a) == 0 || len(b) == 0 {
			return cmp.Compare(len(a), len(b))
		}
		_, _, ret = compareEncodedIDBKeys(a, b)
		if ret != 0 {
			return ret
		}

		return cmp.Compare(sequenceNumberA, sequenceNumberB)
	default:
		panic("invalid key")
	}
}

func (idbCmp1) Name() string {
	return "idb_cmp1"
}

func (idbCmp1) Separator(dst, a, b []byte) []byte {
	return nil
}

func (idbCmp1) Successor(dst, b []byte) []byte {
	return nil
}

// Comparer implements the idb_cmp1 comparer used in Chromium's IndexedDB implementation.
var Comparer comparer.Comparer = idbCmp1{}
