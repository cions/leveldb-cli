# leveldb

[![GitHub Releases](https://img.shields.io/github/v/release/cions/leveldb-cli?sort=semver)](https://github.com/cions/leveldb-cli/releases)
[![LICENSE](https://img.shields.io/github/license/cions/leveldb-cli)](https://github.com/cions/leveldb-cli/blob/master/LICENSE)
[![CI](https://github.com/cions/leveldb-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/cions/leveldb-cli/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/cions/leveldb-cli.svg)](https://pkg.go.dev/github.com/cions/leveldb-cli)

A command-line interface for [LevelDB](https://github.com/google/leveldb). Supports Chromium's IndexedDB databases (`idb_cmp1` comparer).

## Usage

```
leveldb [GLOBAL OPTIONS] COMMAND [ARGS...]
```

`ARGS...` are the arguments specific to each subcommand; see `leveldb help <command>` for details.

### Commands

- `init (i)`: create a new database
- `get (g)`: get the value of the given key
- `put (p)`: set the value of the given key
- `delete (d)`: delete the given keys
- `keys (k)`: list keys only
- `show (s)`: print all entries
- `dump`: export to MessagePack (stream) on stdout
- `load`: import from MessagePack (stream) on stdin
- `repair`: attempt to repair a corrupted DB
- `compact`: run a manual compaction
- `destroy`: remove LevelDB tables in the directory
- `help`: list commands or show command-specific help

### Global options

- `-d, --dbpath=DIR`: database directory (default: `.`) or set `$DBPATH`
- `-i, --indexeddb`: open Chromium IndexedDB databases using the `idb_cmp1` comparer
- `-h, --help`: show help
- `--version`: show version

**When to use `-i`**: Required for Chromium IndexedDB directories, for example:
- `~/.config/chromium/Default/IndexedDB/<origin>_0.indexeddb.leveldb/`
- `chrome-extension_<id>_0.indexeddb.leveldb/`

Do **not** use `-i` for ordinary Bytewise LevelDB databases.

## Installation

[Download from GitHub Releases](https://github.com/cions/leveldb-cli/releases)

### Build from source

```sh
$ go install github.com/cions/leveldb-cli/cmd/leveldb@latest
```

## Quick examples

### Standard (Bytewise) LevelDB

```sh
# initialize an empty DB
leveldb init --dbpath ./demo.db

# write and read keys
leveldb put   --dbpath ./demo.db foo bar
leveldb get   --dbpath ./demo.db foo

# show or dump everything
leveldb show  --dbpath ./demo.db
leveldb dump  --dbpath ./demo.db > demo.dump

# load a dump back (MessagePack streaming format by default)
leveldb load  --dbpath ./demo.db < demo.dump
```

### Chromium IndexedDB (uses `idb_cmp1`)

```sh
# Example: dump a site/extension IndexedDB store
leveldb -i dump --dbpath \
  ~/.config/chromium/Default/IndexedDB/https_example.com_0.indexeddb.leveldb/ \
  > example.dump

# Inspect keys
leveldb -i show --dbpath ~/.config/chromium/.../https_example.com_0.indexeddb.leveldb/

# Repair or compact
leveldb -i repair  --dbpath ~/.config/chromium/.../https_example.com_0.indexeddb.leveldb/
leveldb -i compact --dbpath ~/.config/chromium/.../https_example.com_0.indexeddb.leveldb/
```

## Dump/load formats

- Default format: MessagePack stream (suitable for piping).
- `dump` writes to stdout; redirect to a file. `load` reads from stdin.
- `load` applies records in batches; if interrupted mid-run, some writes may have been applied. Run it on a copy/backup if the data matters.

`keys`, `show`, `dump`, and `delete` honor range options; see `leveldb help <command>`.

## Safety notes

- Always pass `--dbpath` (or set `$DBPATH`) to avoid acting on `.` by default.
- Use `-i` only for IndexedDB stores; using the wrong comparer will fail to open or risk corruption on write/repair.
- Make a backup (`dump`) before `repair`, `compact`, or `destroy` if the data matters.

## License

MIT
