// Copyright (c) 2021-2022 cions
// Licensed under the MIT License. See LICENSE for details

package leveldbcli

import (
	"testing"
)

func TestLevelDBFilenamePattern(t *testing.T) {
	matches := []string{
		"LOCK",
		"LOG",
		"LOG.old",
		"CURRENT",
		"CURRENT.bak",
		"CURRENT.000042",
		"MANIFEST-000042",
		"000042.ldb",
		"000042.log",
		"000042.sst",
		"000042.tmp",
	}

	for _, filename := range matches {
		if !leveldbFilenamePattern.MatchString(filename) {
			t.Errorf("%q should match", filename)
		}
	}

	dontMatches := []string{
		"LOCK2",
		"LOG old",
		"LOG.bak",
		"CURRENT.",
		"CURRENT.orig",
		"CURRENT.-000042",
		"CURRENT.000042a",
		"MANIFEST-",
		"MANIFEST--000042",
		"MANIFEST-000042a",
		".ldb",
		".log",
		".sst",
		".tmp",
		"42ldb",
		"-000042.log",
		"000042a.sst",
		"000042.tmp.gz",
	}

	for _, filename := range dontMatches {
		if leveldbFilenamePattern.MatchString(filename) {
			t.Errorf("%q should not match", filename)
		}
	}
}
