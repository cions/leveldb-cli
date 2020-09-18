package leveldb

import (
	"bytes"
	"os"
	"path"
	"testing"
)

func setupDB(dbpath string) error {
	if err := InitDB(dbpath); err != nil {
		return err
	}
	if err := Put(dbpath, []byte(""), []byte("")); err != nil {
		return err
	}
	if err := Put(dbpath, []byte("key"), []byte("value")); err != nil {
		return err
	}
	return nil
}

func TestInitDB(t *testing.T) {
	tmpdir := t.TempDir()

	if err := InitDB(tmpdir); err != nil {
		t.Errorf("InitDB in an empty directory: unexpected error: %v", err)
	}

	if err := InitDB(tmpdir); err == nil {
		t.Errorf("InitDB in an existing database should fail")
	}
}

func TestGet(t *testing.T) {
	tmpdir := t.TempDir()

	buf := new(bytes.Buffer)

	if err := Get(tmpdir, []byte("key"), buf); err == nil {
		t.Errorf("Get in an empty directory should fail")
	}
	buf.Reset()

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Get(tmpdir, []byte("key"), buf); err != nil {
		t.Errorf("Get(%q): unexpected error: %v", "key", err)
	}
	if !bytes.Equal(buf.Bytes(), []byte("value")) {
		t.Errorf("Get(%q) = %q, got %q", "key", buf.Bytes(), "value")
	}
	buf.Reset()

	if err := Get(tmpdir, []byte("missing"), buf); err == nil {
		t.Errorf("Get(%q) should fail", "missing")
	}
}

func TestPut(t *testing.T) {
	tmpdir := t.TempDir()

	if err := Put(tmpdir, []byte("key"), []byte("value")); err == nil {
		t.Errorf("Put in an empty directory should fail")
	}

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Put(tmpdir, []byte("key"), []byte("updated")); err != nil {
		t.Errorf("Put(%q): unexpected error: %v", "key", err)
	}

	if err := Put(tmpdir, []byte("key2"), []byte("value2")); err != nil {
		t.Errorf("Put(%q): unexpected error: %v", "key2", err)
	}
}

func TestDelete(t *testing.T) {
	tmpdir := t.TempDir()

	if err := Delete(tmpdir, []byte("key")); err == nil {
		t.Errorf("Delete in an empty directory should fail")
	}

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Delete(tmpdir, []byte("key")); err != nil {
		t.Errorf("Delete(%q): unexpected error: %v", "key", err)
	}

	if err := Delete(tmpdir, []byte("missing")); err != nil {
		t.Errorf("Delete(%q): unexpected error: %v", "missing", err)
	}
}

func TestKeys(t *testing.T) {
	tmpdir := t.TempDir()

	buf := new(bytes.Buffer)

	if err := Keys(tmpdir, buf); err == nil {
		t.Errorf("Keys in an empty directory should fail")
	}
	buf.Reset()

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Keys(tmpdir, buf); err != nil {
		t.Errorf("Keys(): unexpected error: %v", err)
	}

	want := "\nkey\n"
	if buf.String() != want {
		t.Errorf("Keys() = %q, want %q", buf.Bytes(), want)
	}
}

func TestShow(t *testing.T) {
	tmpdir := t.TempDir()

	buf := new(bytes.Buffer)

	if err := Show(tmpdir, buf, buf); err == nil {
		t.Errorf("Show in an empty directory should fail")
	}
	buf.Reset()

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Show(tmpdir, buf, buf); err != nil {
		t.Errorf("Show(): unexpected error: %v", err)
	}

	want := ": \nkey: value\n"
	if buf.String() != want {
		t.Errorf("Show() = %q, want %q", buf.Bytes(), want)
	}
}

func TestDump(t *testing.T) {
	tmpdir := t.TempDir()

	buf := new(bytes.Buffer)

	if err := Dump(tmpdir, buf); err == nil {
		t.Errorf("Dump in an empty directory should fail")
	}

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	if err := Dump(tmpdir, buf); err != nil {
		t.Errorf("Dump(): unexpected error: %v", err)
	}

	want := []byte("\x82\xc4\x00\xc4\x00\xc4\x03key\xc4\x05value")
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("Dump() = %q, want %q", buf.Bytes(), want)
	}
}

func TestLoad(t *testing.T) {
	tmpdir := t.TempDir()

	if err := InitDB(tmpdir); err != nil {
		t.Fatalf("Failed to initialize database")
	}

	buf := bytes.NewBufferString("\x82\xc4\x00\xc4\x00\xc4\x03key\xc4\x05value")

	if err := Load(tmpdir, buf); err != nil {
		t.Errorf("Load(): unexpected error: %v", err)
	}

	entries, err := getAll(tmpdir)
	if err != nil {
		t.Errorf("getAll(): unexpected error: %v", err)
	}

	if !bytes.Equal(entries[0].Key, []byte("")) || !bytes.Equal(entries[0].Value, []byte("")) {
		t.Errorf("Load(): failed to load (key %q)", "")
	}

	if !bytes.Equal(entries[1].Key, []byte("key")) || !bytes.Equal(entries[1].Value, []byte("value")) {
		t.Errorf("Load(): failed to load (key %q)", "key")
	}
}

func TestDestroyDB(t *testing.T) {
	tmpdir := t.TempDir()

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	unrelated := path.Join(tmpdir, "unrelated.txt")
	f, err := os.Create(unrelated)
	if err != nil {
		t.Fatalf("Failed to create an unrelated file")
	}
	f.Close()

	if err := DestroyDB(tmpdir); err != nil {
		t.Errorf("DestroyDB(): unexpected error: %v", err)
	}

	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("Stat(%q): unexpected error: %v", "unrelated.txt", err)
	}
}

func TestCompact(t *testing.T) {
	tmpdir := t.TempDir()

	if err := Compact(tmpdir); err == nil {
		t.Errorf("Compact in an empty directory should fail")
	}
	if err := os.Remove(path.Join(tmpdir, "leveldb.bak")); err != nil {
		t.Fatalf("Failed to remove leveldb.bak")
	}

	if err := setupDB(tmpdir); err != nil {
		t.Fatalf("Failed to setup database")
	}

	unrelated := path.Join(tmpdir, "unrelated.txt")
	f, err := os.Create(unrelated)
	if err != nil {
		t.Fatalf("Failed to create an unrelated file")
	}
	f.Close()

	if err := Compact(tmpdir); err != nil {
		t.Errorf("Compact(): unexpected error: %v", err)
	}

	if _, err := os.Stat(unrelated); err != nil {
		t.Errorf("Stat(%q): unexpected error: %v", "unrelated.txt", err)
	}
}

func TestMatchPattern(t *testing.T) {
	cases := []struct {
		pattern, filename string
		result            bool
	}{
		{"LOCK", "LOCK", true},
		{"LOCK", "LOCK.bak", false},
		{"*.log", "42.log", true},
		{"*.log", "-42.log", false},
		{"*.log", "42.log.gz", false},
		{"CURRENT-*", "CURRENT", false},
		{"CURRENT-*", "CURRENT-", false},
		{"CURRENT-*", "CURRENT-20", true},
		{"CURRENT-*", "CURRENT--20", false},
		{"CURRENT-*", "CURRENT-20a", false},
	}

	for _, tc := range cases {
		result := matchPattern(tc.pattern, tc.filename)
		if result != tc.result {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", tc.pattern, tc.filename, result, tc.result)
		}
	}
}

func TestIsLevelDBFilename(t *testing.T) {
	matches := []string{
		"LOCK",
		"LOG",
		"LOG.old",
		"CURRENT",
		"CURRENT.bak",
		"CURRENT.000042",
		"MANIFEST-000042",
		"000042.log",
		"000042.ldb",
		"000042.sst",
		"000042.tmp",
	}

	for _, filename := range matches {
		if !isLevelDBFilename(filename) {
			t.Errorf("%q should match", filename)
		}
	}

	dontMatches := []string{
		"LOCK2",
		"LOG.bak",
		"CURRENT.",
		"CURRENT.orig",
		"CURRENT.-000042",
		"CURRENT.000042a",
		"MANIFEST-",
		"MANIFEST--000042",
		"MANIFEST-000042a",
		".log",
		".ldb",
		".sst",
		".tmp",
		"-000042.ldb",
		"000042a.tmp",
		"000042.log.gz",
	}

	for _, filename := range dontMatches {
		if isLevelDBFilename(filename) {
			t.Errorf("%q should not match", filename)
		}
	}
}
