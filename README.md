# leveldb

[![GitHub Releases](https://img.shields.io/github/downloads/cions/leveldb-cli/latest/total?logo=github)](https://github.com/cions/leveldb-cli/releases)
[![CI](https://github.com/cions/leveldb-cli/workflows/CI/badge.svg)](https://github.com/cions/leveldb-cli/actions)

A command-line interface for [LevelDB](https://github.com/google/leveldb)

## Usage

```sh
$ leveldb init
$ leveldb get <key>
$ leveldb put <key> [<value>]
$ leveldb delete <key>
$ leveldb keys
$ leveldb show
$ leveldb dump
$ leveldb load
$ leveldb compact
$ leveldb destroy
```

## Installation

[Download from GitHub Releases](https://github.com/cions/leveldb-cli/releases)

### Build from source

```sh
$ go get github.com/cions/leveldb-cli/cmd/leveldb
```

# License

MIT
