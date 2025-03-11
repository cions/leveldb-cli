// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package main

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/cions/go-colorterm"
	"github.com/cions/go-options"
	"github.com/cions/leveldb-cli/indexeddb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	NAME    = "leveldb"
	VERSION = "(devel)"
)

var ldbFilesPattern = regexp.MustCompile(`\A(?:LOCK|LOG(?:\.old)?|CURRENT(?:\.bak|\.\d+)?|MANIFEST-\d+|\d+\.(?:ldb|log|sst|tmp))\z`)

func Stringify(src []byte) string {
	return string(src)
}

func EncodeBase64(src []byte) string {
	return base64.StdEncoding.EncodeToString(src)
}

func NewFormatter(quoting, truncating, formatJSON bool) func([]byte) string {
	var (
		Faint = colorterm.EscapeCode("\x1b[2m")
		Reset = colorterm.Reset
	)

	return func(src []byte) string {
		if formatJSON {
			s := src
			for {
				var v *string
				if err := json.Unmarshal(s, &v); err != nil || v == nil {
					break
				}
				if b := []byte(*v); !json.Valid(b) {
					break
				} else {
					s = b
				}
			}

			var v any
			if err := json.Unmarshal(s, &v); err == nil {
				b := new(strings.Builder)
				encoder := json.NewEncoder(b)
				encoder.SetEscapeHTML(false)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(v); err == nil {
					return strings.TrimSuffix(b.String(), "\n")
				}
			}
		}

		s := unsafe.String(unsafe.SliceData(src), len(src))
		b := new(strings.Builder)
		n := 0

		if quoting {
			b.WriteByte('"')
		}
		for i, r := range s {
			switch {
			case r == utf8.RuneError:
				fmt.Fprintf(b, "%v\\x%02X%v", Faint, src[i], Reset)
				n += 4
			case r == '\x00':
				fmt.Fprintf(b, "%v\\0%v", Faint, Reset)
				n += 2
			case r == '\a':
				fmt.Fprintf(b, "%v\\a%v", Faint, Reset)
				n += 2
			case r == '\b':
				fmt.Fprintf(b, "%v\\b%v", Faint, Reset)
				n += 2
			case r == '\f':
				fmt.Fprintf(b, "%v\\f%v", Faint, Reset)
				n += 2
			case r == '\n':
				fmt.Fprintf(b, "%v\\n%v", Faint, Reset)
				n += 2
			case r == '\r':
				fmt.Fprintf(b, "%v\\r%v", Faint, Reset)
				n += 2
			case r == '\t':
				fmt.Fprintf(b, "%v\\t%v", Faint, Reset)
				n += 2
			case r == '\v':
				fmt.Fprintf(b, "%v\\v%v", Faint, Reset)
				n += 2
			case r == '"' && quoting:
				fmt.Fprintf(b, "%v\\\"%v", Faint, Reset)
				n += 2
			case r == '\\':
				fmt.Fprintf(b, "%v\\\\%v", Faint, Reset)
				n += 2
			case unicode.IsPrint(r):
				b.WriteRune(r)
				n += 1
			case r <= 0x7F:
				fmt.Fprintf(b, "%v\\x%02X%v", Faint, r, Reset)
				n += 4
			case r <= 0xFFFF:
				fmt.Fprintf(b, "%v\\u%04X%v", Faint, r, Reset)
				n += 6
			default:
				fmt.Fprintf(b, "%v\\U%08X%v", Faint, r, Reset)
				n += 10
			}
			if truncating && n >= 120 {
				fmt.Fprintf(b, "%v...%v", Faint, Reset)
				break
			}
		}
		if quoting {
			b.WriteByte('"')
		}

		return b.String()
	}
}

func GetParser(raw, base64 bool) func(string) ([]byte, error) {
	switch {
	case base64:
		return ParseBase64
	case raw:
		return ParseRaw
	default:
		return ParseEscaped
	}
}

func ParseRaw(s string) ([]byte, error) {
	return []byte(s), nil
}

func ParseBase64(s string) ([]byte, error) {
	return base64.RawStdEncoding.Strict().DecodeString(strings.TrimRight(s, "="))
}

func ParseEscaped(s string) ([]byte, error) {
	d := make([]byte, 0, len(s))
	for len(s) != 0 {
		if s[0] != '\\' {
			d = append(d, s[0])
			s = s[1:]
			continue
		}
		if len(s) < 2 {
			return nil, fmt.Errorf("trailing backslash escape")
		}
		switch s[1] {
		case '0':
			d = append(d, '\x00')
			s = s[2:]
		case 'a':
			d = append(d, '\a')
			s = s[2:]
		case 'b':
			d = append(d, '\b')
			s = s[2:]
		case 'f':
			d = append(d, '\f')
			s = s[2:]
		case 'n':
			d = append(d, '\n')
			s = s[2:]
		case 'r':
			d = append(d, '\r')
			s = s[2:]
		case 't':
			d = append(d, '\t')
			s = s[2:]
		case 'v':
			d = append(d, '\v')
			s = s[2:]
		case 'x':
			if len(s) < 4 {
				return nil, fmt.Errorf("invalid escape sequence: %s", s)
			}
			n, err := strconv.ParseUint(s[2:4], 16, 8)
			if err != nil {
				return nil, fmt.Errorf("invalid escape sequence: %s", s[:4])
			}
			d = append(d, byte(n))
			s = s[4:]
		case 'u':
			if len(s) < 6 {
				return nil, fmt.Errorf("invalid escape sequence: %s", s)
			}
			n, err := strconv.ParseUint(s[2:6], 16, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid escape sequence: %s", s[:6])
			}
			d = utf8.AppendRune(d, rune(n))
			s = s[6:]
		case 'U':
			if len(s) < 10 {
				return nil, fmt.Errorf("invalid escape sequence: %s", s)
			}
			n, err := strconv.ParseUint(s[2:10], 16, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid escape sequence: %s", s[:10])
			}
			d = utf8.AppendRune(d, rune(n))
			s = s[10:]
		default:
			d = append(d, s[1])
			s = s[2:]
		}
	}
	return d, nil
}

type Entry struct{ Key, Value []byte }

type Matcher interface{ Match(key []byte) bool }

type ConstMatcher bool

func (m ConstMatcher) Match(key []byte) bool { return bool(m) }

type LiteralMatcher [][]byte

func (m LiteralMatcher) Match(key []byte) bool {
	return slices.ContainsFunc(m, func(x []byte) bool {
		return bytes.Equal(key, x)
	})
}

type RegexpMatcher []*regexp.Regexp

func (m RegexpMatcher) Match(key []byte) bool {
	return slices.ContainsFunc(m, func(re *regexp.Regexp) bool {
		return re.Match(key)
	})
}

type HelpEntry struct{ Name, Description string }

func (e HelpEntry) String() string {
	indent := strings.Repeat(" ", 24)
	desc := strings.ReplaceAll(e.Description, "\n", "\n"+indent)
	if len(e.Name) < 20 {
		return fmt.Sprintf("    %-20s%s", e.Name, desc)
	} else {
		return fmt.Sprintf("    %s\n%s%s", e.Name, indent, desc)
	}
}

type HelpParams struct {
	Usage         string
	Description   string
	Commands      []HelpEntry
	Options       []HelpEntry
	GlobalOptions []HelpEntry
}

func (p *HelpParams) Update(child *HelpParams) *HelpParams {
	return &HelpParams{
		Usage:         cmp.Or(child.Usage, p.Usage),
		Description:   cmp.Or(child.Description, p.Description),
		Commands:      nil,
		Options:       slices.Concat(child.Options, p.Options),
		GlobalOptions: slices.Concat(child.GlobalOptions, p.GlobalOptions),
	}
}

func (p *HelpParams) String() string {
	b := new(strings.Builder)
	fmt.Fprintf(b, "Usage: %v [GLOBAL OPTIONS] %v\n\n", NAME, p.Usage)
	fmt.Fprintln(b, p.Description)
	if p.Commands != nil {
		fmt.Fprintln(b, "\nCommands:")
		for _, x := range p.Commands {
			fmt.Fprintln(b, x)
		}
	}
	if p.Options != nil {
		fmt.Fprintln(b, "\nOptions:")
		for _, x := range p.Options {
			fmt.Fprintln(b, x)
		}
	}
	if p.GlobalOptions != nil {
		fmt.Fprintln(b, "\nGlobal Options:")
		for _, x := range p.GlobalOptions {
			fmt.Fprintln(b, x)
		}
	}
	return b.String()
}

type Command interface {
	options.Options
	GetDatabasePath() string
	GetComparer() comparer.Comparer
	HasKeyRange() bool
	GetKeyRange() *util.Range
	Help() *HelpParams
	Run(args []string) error
}

type RootCommand struct {
	DatabasePath string
	IndexedDB    bool
}

func (cmd *RootCommand) Kind(name string) options.Kind {
	switch name {
	case "-d", "--dbpath":
		return options.Required
	case "-i", "--indexeddb":
		return options.Boolean
	case "-h", "--help":
		return options.Boolean
	case "--version":
		return options.Boolean
	default:
		return options.Unknown
	}
}

func (cmd *RootCommand) Option(name, value string, hasValue bool) error {
	switch name {
	case "-d", "--dbpath":
		cmd.DatabasePath = value
	case "-i", "--indexeddb":
		cmd.IndexedDB = true
	case "-h", "--help":
		return options.ErrHelp
	case "--version":
		return options.ErrVersion
	default:
		return options.ErrUnknown
	}
	return nil
}

func (cmd *RootCommand) Subcommand(name string) (Command, error) {
	switch name {
	case "init", "i":
		return &InitCommand{RootCommand: *cmd}, nil
	case "get", "g":
		return &GetCommand{RootCommand: *cmd}, nil
	case "put", "p":
		return &PutCommand{RootCommand: *cmd}, nil
	case "delete", "d":
		return &DeleteCommand{RangedCommand: RangedCommand{RootCommand: *cmd}}, nil
	case "keys", "k":
		return &KeysCommand{RangedCommand: RangedCommand{RootCommand: *cmd}}, nil
	case "show", "s":
		return &ShowCommand{RangedCommand: RangedCommand{RootCommand: *cmd}}, nil
	case "dump":
		return &DumpCommand{RangedCommand: RangedCommand{RootCommand: *cmd}}, nil
	case "load":
		return &LoadCommand{RootCommand: *cmd}, nil
	case "repair":
		return &RepairCommand{RootCommand: *cmd}, nil
	case "compact":
		return &CompactCommand{RootCommand: *cmd}, nil
	case "destroy":
		return &DestroyCommand{RootCommand: *cmd}, nil
	default:
		return nil, options.Errorf("%q is not a valid command. Run '%v --help'", name, NAME)
	}
}

func (cmd *RootCommand) GetDatabasePath() string {
	return cmd.DatabasePath
}

func (cmd *RootCommand) GetComparer() comparer.Comparer {
	if cmd.IndexedDB {
		return indexeddb.Comparer
	}
	return comparer.DefaultComparer
}

func (cmd *RootCommand) HasKeyRange() bool {
	return false
}

func (cmd *RootCommand) GetKeyRange() *util.Range {
	return nil
}

func (cmd *RootCommand) Help() *HelpParams {
	return &HelpParams{
		Usage:       "COMMAND [ARGS...]",
		Description: "A command-line interface for LevelDB",
		Commands: []HelpEntry{
			{"init (i)", "Initialize a database"},
			{"get (g)", "Get the value of the given key"},
			{"put (p)", "Set the value of the given key"},
			{"delete (d)", "Delete the given keys"},
			{"keys (k)", "List all keys"},
			{"show (s)", "Show all entries"},
			{"dump", "Dump the database as MessagePack"},
			{"load", "Load MessagePack into the database"},
			{"repair", "Try to repair the database"},
			{"compact", "Compact the database"},
			{"destroy", "Destroy the database"},
			{"help", "Show a list of commands or help for the command"},
		},
		GlobalOptions: []HelpEntry{
			{"-d, --dbpath=DIR", "Path to the database directory (default: \".\") [$DBPATH]"},
			{"-i, --indexeddb", "Use idb_cmp1 comparer to open Chromium's IndexedDB database"},
			{"-h, --help", "Show this help message and exit"},
			{"    --version", "Show version information and exit"},
		},
	}
}

func (cmd *RootCommand) Run(args []string) error {
	if len(args) == 0 || args[0] == "help" {
		if len(args) < 2 {
			fmt.Print(cmd.Help())
			return nil
		}
		subcmd, err := cmd.Subcommand(args[1])
		if err != nil {
			return err
		}
		fmt.Print(subcmd.Help())
		return nil
	}

	subcmd, err := cmd.Subcommand(args[0])
	if err != nil {
		return err
	}

	subargs, err := options.Parse(subcmd, args[1:])
	switch {
	case errors.Is(err, options.ErrHelp):
		fmt.Print(subcmd.Help())
		return nil
	case err != nil:
		return err
	}

	lockfile := filepath.Join(cmd.GetDatabasePath(), "LOCK")
	if _, err := os.Stat(lockfile); errors.Is(err, fs.ErrNotExist) {
		defer os.Remove(lockfile)
	}

	return subcmd.Run(subargs)
}

type RangedCommand struct {
	RootCommand
	Start  []byte
	Limit  []byte
	Prefix []byte
}

func (cmd *RangedCommand) Kind(name string) options.Kind {
	switch name {
	case "-s", "--start":
		return options.Required
	case "-S", "--start-raw":
		return options.Required
	case "--start-base64":
		return options.Required
	case "-e", "--end":
		return options.Required
	case "-E", "--end-raw":
		return options.Required
	case "--end-base64":
		return options.Required
	case "-p", "--prefix":
		return options.Required
	case "-P", "--prefix-raw":
		return options.Required
	case "--prefix-base64":
		return options.Required
	default:
		return cmd.RootCommand.Kind(name)
	}
}

func (cmd *RangedCommand) Option(name, value string, hasValue bool) (err error) {
	switch name {
	case "-s", "--start":
		cmd.Start, err = ParseEscaped(value)
	case "-S", "--start-raw":
		cmd.Start, err = ParseRaw(value)
	case "--start-base64":
		cmd.Start, err = ParseBase64(value)
	case "-e", "--end":
		cmd.Limit, err = ParseEscaped(value)
	case "-E", "--end-raw":
		cmd.Limit, err = ParseRaw(value)
	case "--end-base64":
		cmd.Limit, err = ParseBase64(value)
	case "-p", "--prefix":
		cmd.Prefix, err = ParseEscaped(value)
	case "-P", "--prefix-raw":
		cmd.Prefix, err = ParseRaw(value)
	case "--prefix-base64":
		cmd.Prefix, err = ParseBase64(value)
	default:
		err = cmd.RootCommand.Option(name, value, hasValue)
	}
	return
}

func (cmd *RangedCommand) HasKeyRange() bool {
	return cmd.Prefix != nil || cmd.Start != nil || cmd.Limit != nil
}

func (cmd *RangedCommand) GetKeyRange() *util.Range {
	if cmd.Prefix != nil {
		if cmd.IndexedDB {
			return indexeddb.Prefix(cmd.Prefix)
		}
		return util.BytesPrefix(cmd.Prefix)
	}
	if cmd.Start != nil || cmd.Limit != nil {
		return &util.Range{Start: cmd.Start, Limit: cmd.Limit}
	}
	return nil
}

func (cmd *RangedCommand) Help() *HelpParams {
	return cmd.RootCommand.Help().Update(&HelpParams{
		Options: []HelpEntry{
			{"-s, --start=KEY", "Start of the key range (inclusive)"},
			{"-S, --start-raw=KEY", "Start of the key range (no escapes, inclusive)"},
			{"    --start-base64=KEY", "Start of the key range (base64, inclusive)"},
			{"-e, --end=KEY", "End of the key range (exclusive)"},
			{"-E, --end-raw=KEY", "End of the key range (no escapes, exclusive)"},
			{"    --end-base64=KEY", "End of the key range (base64, exclusive)"},
			{"-p, --prefix=KEY", "Limit the key range to a range satisfy the given prefix"},
			{"-P, --prefix-raw=KEY", "Limit the key range to a range satisfy the given prefix\n(no escapes)"},
			{"    --prefix-base64=KEY", "Limit the key range to a range satisfy the given prefix\n(base64)"},
		},
	})
}

func main() {
	cmd := &RootCommand{DatabasePath: "."}
	if dbpath := os.Getenv("DBPATH"); dbpath != "" {
		cmd.DatabasePath = dbpath
	}

	args, err := options.ParseS(cmd, os.Args[1:])
	switch {
	case errors.Is(err, options.ErrNoSubcommand):
		err = cmd.Run([]string{"show"})
	case err == nil:
		err = cmd.Run(args)
	}

	switch {
	case errors.Is(err, options.ErrHelp):
		fmt.Print(cmd.Help())
	case errors.Is(err, options.ErrVersion):
		version := VERSION
		if bi, ok := debug.ReadBuildInfo(); ok {
			version = bi.Main.Version
		}
		fmt.Printf("%v %v\n", NAME, version)
	case err != nil:
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", NAME, err)
		if errors.Is(err, options.ErrCmdline) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}
