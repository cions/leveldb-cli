# leveldb

[![GitHub Releases](https://img.shields.io/github/v/release/cions/leveldb-cli?sort=semver)](https://github.com/cions/leveldb-cli/releases)
[![LICENSE](https://img.shields.io/github/license/cions/leveldb-cli)](https://github.com/cions/leveldb-cli/blob/master/LICENSE)
[![CI](https://github.com/cions/leveldb-cli/actions/workflows/ci.yml/badge.svg)](https://github.com/cions/leveldb-cli/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/cions/leveldb-cli.svg)](https://pkg.go.dev/github.com/cions/leveldb-cli)
[![Go Report Card](https://goreportcard.com/badge/github.com/cions/leveldb-cli)](https://goreportcard.com/report/github.com/cions/leveldb-cli)

A command-line interface for [LevelDB](https://github.com/google/leveldb). Supports Chromium's IndexedDB database (`idb_cmp1` comparer).

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
$ leveldb repair
$ leveldb compact
$ leveldb destroy
```

## Installation

[Download from GitHub Releases](https://github.com/cions/leveldb-cli/releases)

### Build from source

```sh
$ go install github.com/cions/leveldb-cli/cmd/leveldb@latest
```

## License

MIT
