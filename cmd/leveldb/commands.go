// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/cions/go-options"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/vmihailenco/msgpack/v5"
)

func OpenDB(
	cmd Command,
	opts *opt.Options,
	visit func(key, value []byte),
	after func(db *leveldb.DB) error,
) error {
	opts.Comparer = cmd.GetComparer()
	db, err := leveldb.OpenFile(cmd.GetDatabasePath(), opts)
	if err != nil {
		return err
	}
	defer db.Close()

	if visit != nil {
		s, err := db.GetSnapshot()
		if err != nil {
			return err
		}
		iter := s.NewIterator(cmd.GetKeyRange(), nil)
		for iter.Next() {
			visit(iter.Key(), iter.Value())
		}
		err = iter.Error()
		iter.Release()
		s.Release()
		if err != nil {
			return err
		}
	}

	if after != nil {
		if err := after(db); err != nil {
			return err
		}
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func DumpDB(cmd Command, w io.Writer) error {
	var entries []Entry
	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
		ReadOnly:       true,
	}, func(key, value []byte) {
		entries = append(entries, Entry{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(value),
		})
	}, func(db *leveldb.DB) error {
		encoder := msgpack.NewEncoder(w)
		encoder.UseCompactInts(true)
		if err := encoder.EncodeMapLen(len(entries)); err != nil {
			return err
		}
		for _, entry := range entries {
			if err := encoder.EncodeBytes(entry.Key); err != nil {
				return err
			}
			if err := encoder.EncodeBytes(entry.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

func LoadDB(cmd Command, r io.Reader) error {
	decoder := msgpack.NewDecoder(r)

	nentries, err := decoder.DecodeMapLen()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	for range nentries {
		key, err := decoder.DecodeBytes()
		if err != nil {
			return err
		}
		value, err := decoder.DecodeBytes()
		if err != nil {
			return err
		}
		batch.Put(key, value)
	}

	return OpenDB(cmd, &opt.Options{}, nil, func(db *leveldb.DB) error {
		return db.Write(batch, nil)
	})
}

func DestroyDB(cmd Command, dryRun bool) error {
	dbpath := cmd.GetDatabasePath()

	entries, err := os.ReadDir(dbpath)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if !ldbFilesPattern.MatchString(entry.Name()) {
			continue
		}
		target := filepath.Join(dbpath, entry.Name())
		if dryRun {
			fmt.Printf("Would remove %s\n", target)
		} else {
			if err := os.Remove(target); err != nil {
				return err
			}
		}
	}

	return nil
}

type InitCommand struct{ RootCommand }

func (cmd *InitCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "init",
		Description: "Initialize a database",
	})
}

func (cmd *InitCommand) Run(args []string) error {
	return OpenDB(cmd, &opt.Options{ErrorIfExist: true}, nil, nil)
}

type GetCommand struct {
	RootCommand
	Raw    bool
	Base64 bool
}

func (cmd *GetCommand) Kind(name string) options.Kind {
	switch name {
	case "-r", "--raw":
		return options.Boolean
	case "-b", "--base64":
		return options.Boolean
	default:
		return cmd.RootCommand.Kind(name)
	}
}

func (cmd *GetCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-r", "--raw":
		cmd.Raw = true
	case "-b", "--base64":
		cmd.Base64 = true
	default:
		return cmd.RootCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *GetCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "get [OPTIONS] KEY",
		Description: "Get the value of the given key",
		Options: []HelpEntry{
			{"-r, --raw", "Do not interpret escape sequences in arguments"},
			{"-b, --base64", "Interpret arguments as base64-encoded"},
		},
	})
}

func (cmd *GetCommand) Run(args []string) error {
	if len(args) == 0 {
		return options.Errorf("no keys to get is given")
	}

	key, err := GetParser(cmd.Raw, cmd.Base64)(args[0])
	if err != nil {
		return options.Errorf("%q: %w", args[0], err)
	}

	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
		ReadOnly:       true,
	}, nil, func(db *leveldb.DB) error {
		value, err := db.Get(key, nil)
		if err != nil {
			return err
		}
		if _, err := os.Stdout.Write(value); err != nil {
			return err
		}
		return nil
	})
}

type PutCommand struct {
	RootCommand
	Raw    bool
	Base64 bool
}

func (cmd *PutCommand) Kind(name string) options.Kind {
	switch name {
	case "-r", "--raw":
		return options.Boolean
	case "-b", "--base64":
		return options.Boolean
	default:
		return cmd.RootCommand.Kind(name)
	}
}

func (cmd *PutCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-r", "--raw":
		cmd.Raw = true
	case "-b", "--base64":
		cmd.Base64 = true
	default:
		return cmd.RootCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *PutCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage: "put [OPTIONS] KEY [VALUE]",
		Description: "Set the value of the given key\n\n" +
			"If VALUE is omitted, the standard input will be read for the value.",
		Options: []HelpEntry{
			{"-r, --raw", "Do not interpret escape sequences in arguments"},
			{"-b, --base64", "Interpret arguments as base64-encoded"},
		},
	})
}

func (cmd *PutCommand) Run(args []string) error {
	if len(args) == 0 {
		return options.Errorf("no keys to set is given")
	}

	var (
		key    []byte
		value  []byte
		err    error
		parser = GetParser(cmd.Raw, cmd.Base64)
	)

	key, err = parser(args[0])
	if err != nil {
		return options.Errorf("%q: %w", args[0], err)
	}

	if len(args) >= 2 {
		value, err = parser(args[1])
		if err != nil {
			return options.Errorf("%q: %w", args[1], err)
		}
	} else {
		value, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
	}

	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
	}, nil, func(db *leveldb.DB) error {
		return db.Put(key, value, nil)
	})
}

type DeleteCommand struct {
	RangedCommand
	Raw    bool
	Base64 bool
	Regexp bool
	Invert bool
	DryRun bool
}

func (cmd *DeleteCommand) Kind(name string) options.Kind {
	switch name {
	case "-r", "--raw":
		return options.Boolean
	case "-b", "--base64":
		return options.Boolean
	case "-R", "--regexp":
		return options.Boolean
	case "-v", "--invert-match":
		return options.Boolean
	case "-n", "--dry-run":
		return options.Boolean
	default:
		return cmd.RangedCommand.Kind(name)
	}
}

func (cmd *DeleteCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-r", "--raw":
		cmd.Raw = true
	case "-b", "--base64":
		cmd.Base64 = true
	case "-R", "--regexp":
		cmd.Regexp = true
	case "-v", "--invert-match":
		cmd.Invert = true
	case "-n", "--dry-run":
		cmd.DryRun = true
	default:
		return cmd.RangedCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *DeleteCommand) Help() *HelpParams {
	return cmd.RangedCommand.Help().Update(&HelpParams{
		Usage: "delete [OPTIONS] KEY...",
		Description: "Delete the given keys\n\n" +
			"NOTE: Key range options limit the overall working range and are applied\n" +
			"before other filters, including --invert-match.",
		Options: []HelpEntry{
			{"-r, --raw", "Do not interpret escape sequences in arguments"},
			{"-b, --base64", "Interpret arguments as base64-encoded"},
			{"-R, --regexp", "Delete keys matching regular expressions"},
			{"-v, --invert-match", "Invert the sense of matching; delete non-matching keys"},
			{"-n, --dry-run", "Do not actually delete; just show what would be deleted"},
		},
	})
}

func (cmd *DeleteCommand) Run(args []string) error {
	var (
		matcher Matcher
		format  = NewFormatter(true, false, false)
		batch   = new(leveldb.Batch)
	)

	switch {
	case len(args) == 0 && !cmd.HasKeyRange():
		return options.Errorf("no keys to delete is given")
	case len(args) == 0:
		matcher = ConstMatcher(true)
	case cmd.Regexp:
		patterns := make([]*regexp.Regexp, len(args))
		for i, arg := range args {
			pattern, err := regexp.Compile(arg)
			if err != nil {
				return options.Errorf("%q: %w", arg, err)
			}
			patterns[i] = pattern
		}
		matcher = RegexpMatcher(patterns)
	default:
		parser := GetParser(cmd.Raw, cmd.Base64)
		keys := make([][]byte, len(args))
		for i, arg := range args {
			key, err := parser(arg)
			if err != nil {
				return options.Errorf("%q: %w", arg, err)
			}
			keys[i] = key
		}
		matcher = LiteralMatcher(keys)
	}

	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
		ReadOnly:       cmd.DryRun,
	}, func(key, value []byte) {
		if matcher.Match(key) == cmd.Invert {
			return
		}
		if cmd.DryRun {
			fmt.Printf("Would delete %v\n", format(key))
		} else {
			batch.Delete(key)
		}
	}, func(db *leveldb.DB) error {
		if cmd.DryRun {
			return nil
		}
		return db.Write(batch, nil)
	})
}

type KeysCommand struct {
	RangedCommand
	Raw    bool
	Base64 bool
}

func (cmd *KeysCommand) Kind(name string) options.Kind {
	switch name {
	case "-r", "--raw":
		return options.Boolean
	case "-b", "--base64":
		return options.Boolean
	default:
		return cmd.RangedCommand.Kind(name)
	}
}

func (cmd *KeysCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-r", "--raw":
		cmd.Raw = true
	case "-b", "--base64":
		cmd.Base64 = true
	default:
		return cmd.RangedCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *KeysCommand) Help() *HelpParams {
	return cmd.RangedCommand.Help().Update(&HelpParams{
		Usage:       "keys [OPTIONS]",
		Description: "List all keys",
		Options: []HelpEntry{
			{"-r, --raw", "Do not escape special characters"},
			{"-b, --base64", "Show keys in base64 encoding"},
		},
	})
}

func (cmd *KeysCommand) Run(args []string) error {
	var format func([]byte) string
	switch {
	case cmd.Base64:
		format = EncodeBase64
	case cmd.Raw:
		format = Stringify
	default:
		format = NewFormatter(false, false, false)
	}

	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
		ReadOnly:       true,
	}, func(key, value []byte) {
		fmt.Println(format(key))
	}, nil)
}

type ShowCommand struct {
	RangedCommand
	Raw        bool
	Base64     bool
	NoTruncate bool
	NoJSON     bool
}

func (cmd *ShowCommand) Kind(name string) options.Kind {
	switch name {
	case "-r", "--raw":
		return options.Boolean
	case "-b", "--base64":
		return options.Boolean
	case "-w", "--no-truncate":
		return options.Boolean
	case "-J", "--no-json":
		return options.Boolean
	default:
		return cmd.RangedCommand.Kind(name)
	}
}

func (cmd *ShowCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-r", "--raw":
		cmd.Raw = true
	case "-b", "--base64":
		cmd.Base64 = true
	case "-w", "--no-truncate":
		cmd.NoTruncate = true
	case "-J", "--no-json":
		cmd.NoJSON = true
	default:
		return cmd.RangedCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *ShowCommand) Help() *HelpParams {
	return cmd.RangedCommand.Help().Update(&HelpParams{
		Usage:       "show [OPTIONS]",
		Description: "Show all entries",
		Options: []HelpEntry{
			{"-r, --raw", "Do not escape special characters"},
			{"-b, --base64", "Show keys and values in base64 encoding"},
			{"-w, --no-truncate", "Do not truncate long values"},
			{"-J, --no-json", "Do not pretty-print JSON values"},
		},
	})
}

func (cmd *ShowCommand) Run(args []string) error {
	var keyf, valf func([]byte) string
	switch {
	case cmd.Base64:
		keyf, valf = EncodeBase64, EncodeBase64
	case cmd.Raw:
		keyf, valf = Stringify, Stringify
	default:
		keyf = NewFormatter(true, false, false)
		valf = NewFormatter(true, !cmd.NoTruncate, !cmd.NoJSON)
	}

	return OpenDB(cmd, &opt.Options{
		ErrorIfMissing: true,
		ReadOnly:       true,
	}, func(key, value []byte) {
		fmt.Printf("%v: %v\n", keyf(key), valf(value))
	}, nil)
}

type DumpCommand struct {
	RangedCommand
	NoClobber bool
}

func (cmd *DumpCommand) Kind(name string) options.Kind {
	switch name {
	case "-n", "--no-clobber":
		return options.Boolean
	default:
		return cmd.RangedCommand.Kind(name)
	}
}

func (cmd *DumpCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-n", "--no-clobber":
		cmd.NoClobber = true
	default:
		return cmd.RangedCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *DumpCommand) Help() *HelpParams {
	return cmd.RangedCommand.Help().Update(&HelpParams{
		Usage:       "dump [OPTIONS] [OUTPUT]",
		Description: "Dump the database as MessagePack",
		Options: []HelpEntry{
			{"-n, --no-clobber", "Do not overwrite an existing file"},
		},
	})
}

func (cmd *DumpCommand) Run(args []string) (err error) {
	var w io.Writer = os.Stdout
	if len(args) != 0 && args[0] != "-" {
		flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
		if cmd.NoClobber {
			flags |= os.O_EXCL
		}
		f, err2 := os.OpenFile(args[0], flags, 0o666)
		if err2 != nil {
			return err2
		}
		defer func() {
			err2 := f.Close()
			err = errors.Join(err, err2)
		}()
		w = f
	}
	return DumpDB(cmd, w)
}

type LoadCommand struct{ RootCommand }

func (cmd *LoadCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "load [INPUT]",
		Description: "Load MessagePack into the database",
	})
}

func (cmd *LoadCommand) Run(args []string) (err error) {
	var r io.Reader = os.Stdin
	if len(args) != 0 && args[0] != "-" {
		f, err2 := os.Open(args[0])
		if err2 != nil {
			return err2
		}
		defer func() {
			err2 := f.Close()
			err = errors.Join(err, err2)
		}()
		r = f
	}
	return LoadDB(cmd, r)
}

type RepairCommand struct{ RootCommand }

func (cmd *RepairCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "repair",
		Description: "Try to repair the database",
	})
}

func (cmd *RepairCommand) Run(args []string) error {
	db, err := leveldb.RecoverFile(cmd.GetDatabasePath(), &opt.Options{
		Comparer: cmd.GetComparer(),
	})
	if err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}
	return nil
}

type CompactCommand struct{ RootCommand }

func (cmd *CompactCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "compact",
		Description: "Compact the database",
	})
}

func (cmd *CompactCommand) Run(args []string) error {
	bakfile := filepath.Join(cmd.GetDatabasePath(), "leveldb.bak")
	bak, err := os.OpenFile(bakfile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer bak.Close()

	if err := DumpDB(cmd, bak); err != nil {
		err2 := bak.Close()
		err3 := os.Remove(bakfile)
		return errors.Join(err, err2, err3)
	}
	if _, err := bak.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := bak.Sync(); err != nil {
		return err
	}
	if err := DestroyDB(cmd, false); err != nil {
		return err
	}
	if err := LoadDB(cmd, bak); err != nil {
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

type DestroyCommand struct {
	RootCommand
	DryRun bool
}

func (cmd *DestroyCommand) Kind(name string) options.Kind {
	switch name {
	case "-n", "--dry-run":
		return options.Boolean
	default:
		return cmd.RootCommand.Kind(name)
	}
}

func (cmd *DestroyCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-n", "--dry-run":
		cmd.DryRun = true
	default:
		return cmd.RootCommand.Option(name, value, hasValue)
	}
	return nil
}

func (cmd *DestroyCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Usage:       "destroy [-n]",
		Description: "Destroy the database",
		Options: []HelpEntry{
			{"-n, --dry-run", "Do not actually remove; just show what would be removed"},
		},
	})
}

func (cmd *DestroyCommand) Run(args []string) error {
	return DestroyDB(cmd, cmd.DryRun)
}
