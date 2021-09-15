package main

import (
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"strings"

	"github.com/cions/leveldb-cli/indexeddb"
	"github.com/syndtr/goleveldb/leveldb/comparer"
	"github.com/urfave/cli/v2"
)

func main() {
	var lockFile string

	version := "(devel)"
	if bi, ok := debug.ReadBuildInfo(); ok {
		version = strings.TrimPrefix(bi.Main.Version, "v")
	}

	app := &cli.App{
		Name:    "leveldb",
		Usage:   "A command-line interface for LevelDB",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "dbpath",
				Aliases: []string{"d"},
				EnvVars: []string{"DBPATH"},
				Value:   ".",
			},
			&cli.BoolFlag{
				Name:    "indexeddb",
				Aliases: []string{"i"},
				Usage:   "Open Chromium's IndexedDB database",
			},
		},
		Before: func(c *cli.Context) error {
			_, err := os.Stat(path.Join(c.String("dbpath"), "LOCK"))
			if os.IsNotExist(err) {
				lockFile = path.Join(c.String("dbpath"), "LOCK")
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			if c.NArg() == 0 {
				return c.App.Command("show").Run(c)
			}
			return c.App.Command("help").Run(c)
		},
		Commands: []*cli.Command{
			{
				Name:      "init",
				Aliases:   []string{"i"},
				Usage:     "initialize a database",
				ArgsUsage: " ",
				Action:    initCmd,
			},
			{
				Name:      "get",
				Aliases:   []string{"g"},
				Usage:     "get the value for the given key",
				ArgsUsage: "<key>",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "raw",
						Aliases: []string{"r"},
						Usage:   "do not interpret backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "interpret arguments as base64-encoded",
					},
				},
				Action: getCmd,
			},
			{
				Name:      "put",
				Aliases:   []string{"p"},
				Usage:     "set the value for the given key",
				ArgsUsage: "<key> [<value>]",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "raw",
						Aliases: []string{"r"},
						Usage:   "do not interpret backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "interpret arguments as base64-encoded",
					},
				},
				Action: putCmd,
			},
			{
				Name:      "delete",
				Aliases:   []string{"d"},
				Usage:     "delete the value for the given key",
				ArgsUsage: "<key>",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "raw",
						Aliases: []string{"r"},
						Usage:   "do not interpret backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "interpret arguments as base64-encoded",
					},
				},
				Action: deleteCmd,
			},
			{
				Name:      "keys",
				Aliases:   []string{"k"},
				Usage:     "list all keys",
				ArgsUsage: " ",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "raw",
						Aliases: []string{"r"},
						Usage:   "do not escape special characters",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "Show keys in base64 encoding",
					},
				},
				Action: keysCmd,
			},
			{
				Name:      "show",
				Aliases:   []string{"s"},
				Usage:     "show all entries",
				ArgsUsage: " ",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "raw",
						Aliases: []string{"r"},
						Usage:   "do not escape special characters",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "Show keys and values in base64 encoding",
					},
					&cli.BoolFlag{
						Name:    "no-json",
						Aliases: []string{"J"},
						Usage:   "do not pretty-print JSON values",
					},
					&cli.BoolFlag{
						Name:    "no-truncate",
						Aliases: []string{"w"},
						Usage:   "do not truncate output",
					},
				},
				UseShortOptionHandling: true,
				Action:                 showCmd,
			},
			{
				Name:      "dump",
				Usage:     "dump the database as MessagePack",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					var cmp comparer.Comparer = comparer.DefaultComparer
					if c.Bool("indexeddb") {
						cmp = indexeddb.IndexedDBComparer
					}
					return dumpDB(c.String("dbpath"), cmp, os.Stdout)
				},
			},
			{
				Name:      "load",
				Usage:     "load MessagePack into the database",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					var cmp comparer.Comparer = comparer.DefaultComparer
					if c.Bool("indexeddb") {
						cmp = indexeddb.IndexedDBComparer
					}
					return loadDB(c.String("dbpath"), cmp, os.Stdin)
				},
			},
			{
				Name:      "compact",
				Usage:     "compact the database",
				ArgsUsage: " ",
				Action:    compactCmd,
			},
			{
				Name:      "destroy",
				Usage:     "destroy the database",
				ArgsUsage: " ",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:    "dry-run",
						Aliases: []string{"n"},
						Usage:   "do not actually remove anything, just show what would be done",
					},
				},
				Action: func(c *cli.Context) error {
					return destroyDB(c.String("dbpath"), c.Bool("dry-run"))
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		if lockFile != "" {
			os.Remove(lockFile)
		}
		fmt.Fprintf(os.Stderr, "leveldb: error: %v\n", err)
		os.Exit(1)
	}
}
