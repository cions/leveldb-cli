package main

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime/debug"
	"strings"

	"github.com/cions/leveldb-cli"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func main() {
	var lockFile string

	var version string = "(devel)"
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
				Action: func(c *cli.Context) error {
					return leveldb.InitDB(c.String("dbpath"))
				},
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
						Usage:   "do not recognize backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "treat arguments as base64-encoded",
					},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						cli.ShowCommandHelpAndExit(c, "get", 2)
					}

					var err error
					key := []byte(c.Args().Get(0))
					if c.Bool("base64") {
						key, err = leveldb.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = leveldb.Unescape(key)
					}
					if err != nil {
						return err
					}
					return leveldb.Get(c.String("dbpath"), key, os.Stdout)
				},
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
						Usage:   "do not recognize backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "treat arguments as base64-encoded",
					},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						cli.ShowCommandHelpAndExit(c, "put", 2)
					}

					var err error
					dbpath := c.String("dbpath")

					key := []byte(c.Args().Get(0))
					if c.Bool("base64") {
						key, err = leveldb.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = leveldb.Unescape(key)
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
							value, err = leveldb.DecodeBase64(value)
						} else if !c.Bool("raw") {
							value, err = leveldb.Unescape(value)
						}
					}
					if err != nil {
						return err
					}
					return leveldb.Put(dbpath, key, value)
				},
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
						Usage:   "do not recognize backslash escapes",
					},
					&cli.BoolFlag{
						Name:    "base64",
						Aliases: []string{"b"},
						Usage:   "treat arguments as base64-encoded",
					},
				},
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						cli.ShowCommandHelpAndExit(c, "delete", 2)
					}

					var err error
					key := []byte(c.Args().Get(0))
					if c.Bool("base64") {
						key, err = leveldb.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = leveldb.Unescape(key)
					}
					if err != nil {
						return err
					}
					return leveldb.Delete(c.String("dbpath"), key)
				},
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
						Usage:   "encode keys in base64",
					},
				},
				Action: func(c *cli.Context) error {
					var w io.Writer = os.Stdout
					if c.Bool("base64") {
						w = leveldb.NewBase64Writer(os.Stdout)
					} else if !c.Bool("raw") {
						w = leveldb.NewPrettyPrinter(color.Output)
					}
					return leveldb.Keys(c.String("dbpath"), w)
				},
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
						Usage:   "base64 encode",
					},
					&cli.BoolFlag{
						Name:    "no-json",
						Aliases: []string{"J"},
						Usage:   "do not pretty-print JSON values",
					},
					&cli.BoolFlag{
						Name:    "wide",
						Aliases: []string{"w"},
						Usage:   "do not truncate value",
					},
				},
				UseShortOptionHandling: true,
				Action: func(c *cli.Context) error {
					var kw, vw io.Writer = os.Stdout, os.Stdout
					if c.Bool("base64") {
						kw = leveldb.NewBase64Writer(os.Stdout)
						vw = leveldb.NewBase64Writer(os.Stdout)
					} else if !c.Bool("raw") {
						kw = leveldb.NewPrettyPrinter(color.Output).SetQuoting(true)
						vw = leveldb.NewPrettyPrinter(color.Output).
							SetQuoting(true).
							SetTruncate(!c.Bool("wide")).
							SetParseJSON(!c.Bool("no-json"))
					}
					return leveldb.Show(c.String("dbpath"), kw, vw)
				},
			},
			{
				Name:      "dump",
				Usage:     "dump all entries as MessagePack",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					return leveldb.Dump(c.String("dbpath"), os.Stdout)
				},
			},
			{
				Name:      "load",
				Usage:     "load MessagePack into the database",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					return leveldb.Load(c.String("dbpath"), os.Stdin)
				},
			},
			{
				Name:      "compact",
				Usage:     "compact the database",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					return leveldb.Compact(c.String("dbpath"))
				},
			},
			{
				Name:      "destroy",
				Usage:     "destroy the database",
				ArgsUsage: " ",
				Action: func(c *cli.Context) error {
					return leveldb.DestroyDB(c.String("dbpath"))
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
