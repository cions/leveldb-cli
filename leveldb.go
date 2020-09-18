package leveldb

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/vmihailenco/msgpack"
)

type Entry struct {
	Key, Value []byte
}

func getAll(dbpath string) ([]Entry, error) {
	opts := &opt.Options{ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	s, err := db.GetSnapshot()
	if err != nil {
		return nil, err
	}

	var result []Entry

	iter := s.NewIterator(nil, nil)
	for iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		result = append(result, Entry{
			Key:   key,
			Value: value,
		})
	}
	iter.Release()
	err = iter.Error()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func InitDB(dbpath string) error {
	opts := &opt.Options{ErrorIfExist: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	return db.Close()
}

func Get(dbpath string, key []byte, w io.Writer) error {
	opts := &opt.Options{ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	value, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	_, err = w.Write(value)
	return err
}

func Put(dbpath string, key, value []byte) error {
	opts := &opt.Options{ErrorIfMissing: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Put(key, value, nil)
}

func Delete(dbpath string, key []byte) error {
	opts := &opt.Options{ErrorIfMissing: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.Delete(key, nil)
}

func Keys(dbpath string, w io.Writer) error {
	data, err := getAll(dbpath)
	if err != nil {
		return err
	}

	var rw io.Writer = w
	if wu, ok := w.(interface{ Unwrap() io.Writer }); ok {
		rw = wu.Unwrap()
	}

	for _, entry := range data {
		if _, err := w.Write(entry.Key); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(rw); err != nil {
			return err
		}
	}

	return nil
}

func Show(dbpath string, kw, vw io.Writer) error {
	data, err := getAll(dbpath)
	if err != nil {
		return err
	}

	var rw io.Writer = kw
	if wu, ok := kw.(interface{ Unwrap() io.Writer }); ok {
		rw = wu.Unwrap()
	}

	for _, entry := range data {
		kw.Write(entry.Key)
		fmt.Fprint(rw, ": ")
		vw.Write(entry.Value)
		fmt.Fprintln(rw)
	}

	return nil
}

func Dump(dbpath string, w io.Writer) error {
	data, err := getAll(dbpath)
	if err != nil {
		return err
	}

	enc := msgpack.NewEncoder(w).UseCompactEncoding(true)
	if err := enc.EncodeMapLen(len(data)); err != nil {
		return err
	}

	for _, entry := range data {
		if err := enc.EncodeBytes(entry.Key); err != nil {
			return err
		}
		if err := enc.EncodeBytes(entry.Value); err != nil {
			return err
		}
	}

	return nil
}

func Load(dbpath string, r io.Reader) error {
	dec := msgpack.NewDecoder(r)

	nentry, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}

	data := make([]Entry, 0, nentry)
	for i := 0; i < nentry; i++ {
		key, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		value, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		data = append(data, Entry{Key: key, Value: value})
	}

	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	batch := new(leveldb.Batch)
	for _, entry := range data {
		batch.Put(entry.Key, entry.Value)
	}
	return db.Write(batch, nil)
}

func matchPattern(pattern, filename string) bool {
	i, j := 0, 0
	for i < len(pattern) && j < len(filename) {
		if pattern[i] == '*' {
			if !('0' <= filename[j] && filename[j] <= '9') {
				return false
			}
			j += 1
			for j < len(filename) && '0' <= filename[j] && filename[j] <= '9' {
				j += 1
			}
			i += 1
		} else if pattern[i] != filename[j] {
			return false
		} else {
			i += 1
			j += 1
		}
	}
	return i == len(pattern) && j == len(filename)
}

func isLevelDBFilename(filename string) bool {
	patterns := []string{
		"LOCK",
		"LOG",
		"LOG.old",
		"CURRENT",
		"CURRENT.bak",
		"CURRENT.*",
		"MANIFEST-*",
		"*.ldb",
		"*.log",
		"*.sst",
		"*.tmp",
	}

	for _, pattern := range patterns {
		if matchPattern(pattern, filename) {
			return true
		}
	}
	return false
}

func DestroyDB(dbpath string) error {
	dir, err := os.Open(dbpath)
	if err != nil {
		return err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	for _, name := range names {
		if !isLevelDBFilename(name) {
			continue
		}
		if err := os.Remove(path.Join(dbpath, name)); err != nil {
			return err
		}
	}

	return nil
}

func Compact(dbpath string) error {
	bakfile := path.Join(dbpath, "leveldb.bak")

	bak, err := os.OpenFile(bakfile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer bak.Close()

	if err := Dump(dbpath, bak); err != nil {
		return err
	}

	if _, err := bak.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	if err := DestroyDB(dbpath); err != nil {
		return err
	}

	if err := Load(dbpath, bak); err != nil {
		return err
	}

	return os.Remove(bakfile)
}
