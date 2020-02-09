package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/cions/leveldb-cli"
	"github.com/cions/leveldb-cli/internal"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

func main() {
	var lockFile string

	app := &cli.App{
		Name:    "ldb",
		Usage:   "A command-line interface for LevelDB",
		Version: "0.1.0",
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
						key, err = internal.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = internal.Unquote(key)
					}
					if err != nil {
						return err
					}
					return leveldb.Get(c.String("dbpath"), key)
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
						key, err = internal.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = internal.Unquote(key)
					}
					if err != nil {
						return err
					}

					var value []byte
					if c.NArg() == 1 {
						value, err = ioutil.ReadAll(os.Stdin)
					} else {
						value = []byte(c.Args().Get(1))
						if c.Bool("base64") {
							value, err = internal.DecodeBase64(value)
						} else if !c.Bool("raw") {
							value, err = internal.Unquote(value)
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
						key, err = internal.DecodeBase64(key)
					} else if !c.Bool("raw") {
						key, err = internal.Unquote(key)
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
						w = internal.NewBase64Writer(os.Stdout)
					} else if !c.Bool("raw") {
						w = internal.NewQuotingWriter(color.Output).SetWide(true)
					}
					return leveldb.Keys(c.String("dbpath"), w)
				},
			},
			{
				Name:      "show",
				Aliases:   []string{"s"},
				Usage:     "show all entries",
				ArgsUsage: "",
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
						kw = internal.NewBase64Writer(os.Stdout)
						vw = internal.NewBase64Writer(os.Stdout)
					} else if !c.Bool("raw") {
						kw = internal.NewQuotingWriter(color.Output).
							SetDoubleQuote(true).
							SetWide(true)
						vw = internal.NewQuotingWriter(color.Output).
							SetDoubleQuote(true).
							SetParseJSON(!c.Bool("no-json")).
							SetWide(c.Bool("wide"))
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

	err := app.Run(os.Args)
	if err != nil {
		if lockFile != "" {
			os.Remove(lockFile)
		}
		fmt.Fprintf(os.Stderr, "ldb: %v\n", err)
		os.Exit(1)
	}
}
