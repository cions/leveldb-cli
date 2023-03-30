// Copyright (c) 2021-2023 cions
// Licensed under the MIT License. See LICENSE for details

package leveldbcli

import (
	"os"
	"path"
	"runtime/debug"
	"strings"

	"github.com/urfave/cli/v2"
)

// Main runs the command
func Main(args []string) error {
	version := "(devel)"
	if bi, ok := debug.ReadBuildInfo(); ok {
		version = strings.TrimPrefix(bi.Main.Version, "v")
	}

	var lockFile string

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
				Usage:   "path to the database `dir`ectory",
			},
			&cli.BoolFlag{
				Name:    "indexeddb",
				Aliases: []string{"i"},
				Usage:   "open Chromium's IndexedDB database",
			},
		},
		UseShortOptionHandling: true,
		Before: func(c *cli.Context) error {
			p := path.Join(c.String("dbpath"), "LOCK")
			if _, err := os.Stat(p); os.IsNotExist(err) {
				lockFile = p
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
						Usage:   "show keys in base64 encoding",
					},
					&cli.StringFlag{
						Name:    "start",
						Aliases: []string{"s"},
						Usage:   "start of the `key` range (inclusive)",
					},
					&cli.StringFlag{
						Name:    "start-raw",
						Aliases: []string{"S"},
						Usage:   "start of the `key` range (no backslash escapes, inclusive)",
					},
					&cli.StringFlag{
						Name:  "start-base64",
						Usage: "start of the `key` range (in base64, inclusive)",
					},
					&cli.StringFlag{
						Name:    "end",
						Aliases: []string{"e"},
						Usage:   "end of the `key` range (exclusive)",
					},
					&cli.StringFlag{
						Name:    "end-raw",
						Aliases: []string{"E"},
						Usage:   "end of the `key` range (no backslash escapes, exclusive)",
					},
					&cli.StringFlag{
						Name:  "end-base64",
						Usage: "end of the `key` range (in base64, exclusive)",
					},
					&cli.StringFlag{
						Name:    "prefix",
						Aliases: []string{"p"},
						Usage:   "limit the key range to a range that satisfy the given `prefix`",
					},
					&cli.StringFlag{
						Name:    "prefix-raw",
						Aliases: []string{"P"},
						Usage:   "limit the key range to a range that satisfy the given `prefix` (no backslash escapes)",
					},
					&cli.StringFlag{
						Name:  "prefix-base64",
						Usage: "limit the key range to a range that satisfy the given `prefix` (in base64)",
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
						Usage:   "show keys and values in base64 encoding",
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
					&cli.StringFlag{
						Name:    "start",
						Aliases: []string{"s"},
						Usage:   "start of the `key` range (inclusive)",
					},
					&cli.StringFlag{
						Name:    "start-raw",
						Aliases: []string{"S"},
						Usage:   "start of the `key` range (no backslash escapes, inclusive)",
					},
					&cli.StringFlag{
						Name:  "start-base64",
						Usage: "start of the `key` range (in base64, inclusive)",
					},
					&cli.StringFlag{
						Name:    "end",
						Aliases: []string{"e"},
						Usage:   "end of the `key` range (exclusive)",
					},
					&cli.StringFlag{
						Name:    "end-raw",
						Aliases: []string{"E"},
						Usage:   "end of the `key` range (no backslash escapes, exclusive)",
					},
					&cli.StringFlag{
						Name:  "end-base64",
						Usage: "end of the `key` range (in base64, exclusive)",
					},
					&cli.StringFlag{
						Name:    "prefix",
						Aliases: []string{"p"},
						Usage:   "limit the key range to a range that satisfy the given `prefix`",
					},
					&cli.StringFlag{
						Name:    "prefix-raw",
						Aliases: []string{"P"},
						Usage:   "limit the key range to a range that satisfy the given `prefix` (no backslash escapes)",
					},
					&cli.StringFlag{
						Name:  "prefix-base64",
						Usage: "limit the key range to a range that satisfy the given `prefix` (in base64)",
					},
				},
				UseShortOptionHandling: true,
				Action:                 showCmd,
			},
			{
				Name:      "dump",
				Usage:     "dump the database as MessagePack",
				ArgsUsage: " ",
				Action:    dumpCmd,
			},
			{
				Name:      "load",
				Usage:     "load MessagePack into the database",
				ArgsUsage: " ",
				Action:    loadCmd,
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
				Action: destroyCmd,
			},
		},
	}

	if err := app.Run(args); err != nil {
		if lockFile != "" {
			os.Remove(lockFile)
		}
		return err
	}

	return nil
}
