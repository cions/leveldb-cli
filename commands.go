package leveldbcli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"

	"github.com/cions/leveldb-cli/indexeddb"
	"github.com/fatih/color"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/urfave/cli/v2"
	"github.com/vmihailenco/msgpack/v5"
)

type entry struct {
	Key, Value []byte
}

var leveldbFilenamePattern = regexp.MustCompile(`^(?:LOCK|LOG(?:\.old)?|CURRENT(?:\.bak|\.\d+)?|MANIFEST-\d+|\d+\.(?:ldb|log|sst|tmp))$`)

func initCmd(c *cli.Context) error {
	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfExist: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func getCmd(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		cli.ShowCommandHelpAndExit(c, "get", 2)
	}

	key := []byte(c.Args().Get(0))
	if c.Bool("base64") {
		key, err = decodeBase64(key)
	} else if !c.Bool("raw") {
		key, err = unescape(key)
	}
	if err != nil {
		return err
	}

	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	value, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	if _, err := os.Stdout.Write(value); err != nil {
		return err
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func putCmd(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		cli.ShowCommandHelpAndExit(c, "put", 2)
	}

	key := []byte(c.Args().Get(0))
	if c.Bool("base64") {
		key, err = decodeBase64(key)
	} else if !c.Bool("raw") {
		key, err = unescape(key)
	}
	if err != nil {
		return err
	}

	var value []byte
	if c.NArg() == 1 {
		value, err = io.ReadAll(os.Stdin)
	} else {
		value = []byte(c.Args().Get(1))
		if c.Bool("base64") {
			value, err = decodeBase64(value)
		} else if !c.Bool("raw") {
			value, err = unescape(value)
		}
	}
	if err != nil {
		return err
	}

	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Put(key, value, nil); err != nil {
		return err
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func deleteCmd(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		cli.ShowCommandHelpAndExit(c, "delete", 2)
	}

	key := []byte(c.Args().Get(0))
	if c.Bool("base64") {
		key, err = decodeBase64(key)
	} else if !c.Bool("raw") {
		key, err = unescape(key)
	}
	if err != nil {
		return err
	}

	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Delete(key, nil); err != nil {
		return err
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func getKeyRange(c *cli.Context) (*util.Range, error) {
	if c.IsSet("prefix-base64") {
		prefix, err := decodeBase64([]byte(c.String("prefix-base64")))
		if err != nil {
			return nil, fmt.Errorf("option --prefix-base64: %w", err)
		}
		if c.Bool("indexeddb") {
			return indexeddb.Prefix(prefix), nil
		}
		return util.BytesPrefix(prefix), nil
	}
	if c.IsSet("prefix-raw") {
		prefix := []byte(c.String("prefix-raw"))
		if c.Bool("indexeddb") {
			return indexeddb.Prefix(prefix), nil
		}
		return util.BytesPrefix(prefix), nil
	}
	if c.IsSet("prefix") {
		prefix, err := unescape([]byte(c.String("prefix")))
		if err != nil {
			return nil, fmt.Errorf("option --prefix: %w", err)
		}
		if c.Bool("indexeddb") {
			return indexeddb.Prefix(prefix), nil
		}
		return util.BytesPrefix(prefix), nil
	}

	slice := &util.Range{}

	if c.IsSet("start-base64") {
		start, err := decodeBase64([]byte(c.String("start-base64")))
		if err != nil {
			return nil, fmt.Errorf("option --start-base64: %w", err)
		}
		slice.Start = start
	} else if c.IsSet("start-raw") {
		slice.Start = []byte(c.String("start-raw"))
	} else if c.IsSet("start") {
		start, err := unescape([]byte(c.String("start")))
		if err != nil {
			return nil, fmt.Errorf("option --start: %w", err)
		}
		slice.Start = start
	}

	if c.IsSet("end-base64") {
		end, err := decodeBase64([]byte(c.String("end-base64")))
		if err != nil {
			return nil, fmt.Errorf("option --end-base64: %w", err)
		}
		slice.Limit = end
	} else if c.IsSet("end-raw") {
		slice.Limit = []byte(c.String("end-raw"))
	} else if c.IsSet("end") {
		end, err := unescape([]byte(c.String("end")))
		if err != nil {
			return nil, fmt.Errorf("option --end: %w", err)
		}
		slice.Limit = end
	}

	if slice.Start != nil && slice.Limit != nil {
		var cmp func([]byte, []byte) int = bytes.Compare
		if c.Bool("indexeddb") {
			cmp = indexeddb.IndexedDBComparer.Compare
		}
		if cmp(slice.Start, slice.Limit) > 0 {
			slice.Limit = slice.Start
		}
	}

	if slice.Start == nil && slice.Limit == nil {
		return nil, nil
	}
	return slice, nil
}

func keysCmd(c *cli.Context) error {
	var w io.Writer = os.Stdout
	if c.Bool("base64") {
		w = newBase64Writer(os.Stdout)
	} else if !c.Bool("raw") {
		w = newPrettyPrinter(color.Output)
	}

	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	slice, err := getKeyRange(c)
	if err != nil {
		return err
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	s, err := db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	iter := s.NewIterator(slice, nil)
	defer iter.Release()
	for iter.Next() {
		if _, err := w.Write(iter.Key()); err != nil {
			return err
		}
		if _, err := os.Stdout.WriteString("\n"); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	iter.Release()
	s.Release()
	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func showCmd(c *cli.Context) error {
	var kw, vw io.Writer = os.Stdout, os.Stdout
	if c.Bool("base64") {
		kw = newBase64Writer(os.Stdout)
		vw = newBase64Writer(os.Stdout)
	} else if !c.Bool("raw") {
		kw = newPrettyPrinter(color.Output).SetQuoting(true)
		vw = newPrettyPrinter(color.Output).
			SetQuoting(true).
			SetTruncate(!c.Bool("no-truncate")).
			SetParseJSON(!c.Bool("no-json"))
	}

	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	slice, err := getKeyRange(c)
	if err != nil {
		return err
	}

	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(c.String("dbpath"), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	s, err := db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	iter := s.NewIterator(slice, nil)
	defer iter.Release()
	for iter.Next() {
		if _, err := kw.Write(iter.Key()); err != nil {
			return err
		}
		if _, err := os.Stdout.WriteString(": "); err != nil {
			return err
		}
		if _, err := vw.Write(iter.Value()); err != nil {
			return err
		}
		if _, err := os.Stdout.WriteString("\n"); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	iter.Release()
	s.Release()
	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func dumpDB(dbpath string, cmp comparer.Comparer, w io.Writer) error {
	opts := &opt.Options{Comparer: cmp, ErrorIfMissing: true, ReadOnly: true}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	s, err := db.GetSnapshot()
	if err != nil {
		return err
	}
	defer s.Release()

	var entries []entry
	iter := s.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		value := make([]byte, len(iter.Value()))
		copy(value, iter.Value())
		entries = append(entries, entry{Key: key, Value: value})
	}
	if err := iter.Error(); err != nil {
		return err
	}

	enc := msgpack.NewEncoder(w)
	enc.UseCompactInts(true)
	if err := enc.EncodeMapLen(len(entries)); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := enc.EncodeBytes(entry.Key); err != nil {
			return err
		}
		if err := enc.EncodeBytes(entry.Value); err != nil {
			return err
		}
	}

	iter.Release()
	s.Release()
	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func loadDB(dbpath string, cmp comparer.Comparer, r io.Reader) error {
	dec := msgpack.NewDecoder(r)

	nentries, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}

	entries := make([]entry, nentries)
	for i := 0; i < nentries; i++ {
		key, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		value, err := dec.DecodeBytes()
		if err != nil {
			return err
		}
		entries[i].Key = key
		entries[i].Value = value
	}

	opts := &opt.Options{Comparer: cmp}
	db, err := leveldb.OpenFile(dbpath, opts)
	if err != nil {
		return err
	}
	defer db.Close()

	batch := new(leveldb.Batch)
	for _, entry := range entries {
		batch.Put(entry.Key, entry.Value)
	}
	if err := db.Write(batch, nil); err != nil {
		return err
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func destroyDB(dbpath string, dryRun bool) error {
	dir, err := os.Open(dbpath)
	if err != nil {
		return err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(0)
	if err != nil {
		return err
	}

	for _, filename := range names {
		if !leveldbFilenamePattern.MatchString(filename) {
			continue
		}
		target := path.Join(dbpath, filename)
		if dryRun {
			fmt.Printf("Would remove %s\n", target)
			continue
		}
		if err := os.Remove(target); err != nil {
			return err
		}
	}

	if err := dir.Close(); err != nil {
		return err
	}

	return nil
}

func dumpCmd(c *cli.Context) error {
	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}
	return dumpDB(c.String("dbpath"), cmp, os.Stdout)
}

func loadCmd(c *cli.Context) error {
	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}
	return loadDB(c.String("dbpath"), cmp, os.Stdin)
}

func compactCmd(c *cli.Context) error {
	dbpath := c.String("dbpath")
	bakfile := path.Join(dbpath, "leveldb.bak")
	var cmp comparer.Comparer = comparer.DefaultComparer
	if c.Bool("indexeddb") {
		cmp = indexeddb.IndexedDBComparer
	}

	bak, err := os.OpenFile(bakfile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer bak.Close()
	if err := dumpDB(dbpath, cmp, bak); err != nil {
		return err
	}
	if _, err := bak.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := bak.Sync(); err != nil {
		return err
	}
	if err := destroyDB(dbpath, false); err != nil {
		return err
	}
	if err := loadDB(dbpath, cmp, bak); err != nil {
		return err
	}
	if err := bak.Close(); err != nil {
		return err
	}
	if err := os.Remove(bakfile); err != nil {
		return err
	}

	return nil
}

func destroyCmd(c *cli.Context) error {
	return destroyDB(c.String("dbpath"), c.Bool("dry-run"))
}
