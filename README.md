# ldb

A command-line interface for [LevelDB](https://github.com/google/leveldb).

# Usage

```sh
$ ldb [-d <DBPATH>] init
$ ldb [-d <DBPATH>] get <key>
$ ldb [-d <DBPATH>] put <key> [<value>]
$ ldb [-d <DBPATH>] delete <key>
$ ldb [-d <DBPATH>] keys
$ ldb [-d <DBPATH>] show
$ ldb [-d <DBPATH>] dump
$ ldb [-d <DBPATH>] load
$ ldb [-d <DBPATH>] compact
$ ldb [-d <DBPATH>] destroy
```

# Installation

```sh
$ go get github.com/cions/leveldb-cli/cmd/ldb
```

# License

MIT
