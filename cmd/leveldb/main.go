package main

import (
	"fmt"
	"os"

	leveldbcli "github.com/cions/leveldb-cli"
)

func main() {
	if err := leveldbcli.Main(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "leveldb: error: %v\n", err)
		os.Exit(1)
	}
}
