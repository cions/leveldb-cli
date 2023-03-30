// Copyright (c) 2021-2023 cions
// Licensed under the MIT License. See LICENSE for details

package indexeddb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/util"
)

func decodeHex(s string) []byte {
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		panic(fmt.Sprintf(`decodeHex("%s"): %v`, s, err))
	}
	return b
}

func TestPrefix(t *testing.T) {
	cases := []struct {
		Prefix, Start, Limit string
	}{
		{"", "", ""},

		{"00", "00 00 00 00", "01 ff ff 0001"},
		{"01", "01 00 00 0001", "02 ff ff 000001"},
		{"03", "03 00 00 00000001", "04 ff 0001 00"},
		{"04", "04 00 0001 00", "05 ff ffff 0001"},
		{"1c", "1c 00 0000000000000001 00", "1d ff ffffffffffffff7f 0001"},
		{"1f", "1f 00 0000000000000001 00000001", "20 0001 00 00"},
		{"20", "20 0001 00 00", "21 ffff ff 0001"},
		{"e0", "e0 0000000000000001 00 00", "e1 ffffffffffffff7f ff 0001"},
		{"ff", "ff 0000000000000001 0000000000000001 00000001", ""},

		{"25 00", "25 0001 0001 0001", "26 00ff ffff 000001"},
		{"25 ff", "25 ff01 0001 0001", "26 ffff ffff 000001"},
		{"25 ffff", "25 ffff 0001 0001", "26 ffff ffff 000001"},
		{"25 ffff 00", "25 ffff 0001 0001", "26 ffff 00ff 000001"},
		{"25 ffff ff", "25 ffff ff01 0001", "26 ffff ffff 000001"},
		{"25 ffff ffff", "25 ffff ffff 0001", "26 ffff ffff 000001"},
		{"25 ffff ffff 00", "25 ffff ffff 0001", "25 ffff ffff 01ff"},
		{"25 ffff ffff ff", "25 ffff ffff ff01", "26 ffff ffff 000001"},
		{"ff 0123", "ff 0123000000000001 0000000000000001 00000001", "e0 0223ffffffffff7f 00 00"},
		{"ff ffff", "ff ffff000000000001 0000000000000001 00000001", ""},
		{"ff ffffffffffffff7f 0123", "ff ffffffffffffff7f 0123000000000001 00000001", "fc ffffffffffffff7f 0223ffffffffff7f 00"},
		{"ff ffffffffffffff7f ffff", "ff ffffffffffffff7f ffff000000000001 00000001", ""},
		{"ff ffffffffffffff7f ffffffffffffff7f 0123", "ff ffffffffffffff7f ffffffffffffff7f 01230001", "ff ffffffffffffff7f ffffffffffffff7f 0223ffff"},
		{"ff ffffffffffffff7f ffffffffffffff7f ffff", "ff ffffffffffffff7f ffffffffffffff7f ffff0001", ""},

		{"00 00 00 00", "00 00 00 00", "00 00 00 01"},
		{"00 00 00 ff", "00 00 00 ff", "01 00 00 0001"},
		{"03 00 00 ffffffff", "03 00 00 ffffffff", "00 00 01 00"},
		{"1f 00 ffffffffffffff7f ffffffff", "1f 00 ffffffffffffff7f ffffffff", "00 01 00 00"},
		{"ff ffffffffffffff7f ffffffffffffff7f ffffffff", "ff ffffffffffffff7f ffffffffffffff7f ffffffff", ""},

		{"00 00 00 00 00", "00 00 00 00 00", "00 00 00 00 01"},
		{"00 00 00 00 32", "00 00 00 00 32", "00 00 00 00 33"},
		{"00 00 00 00 32 01", "00 00 00 00 32 01", "00 00 00 00 32 02"},
		{"00 00 00 00 32 ff", "00 00 00 00 32 ff", "00 00 00 00 33"},
		{"00 00 00 00 64", "00 00 00 00 64", "00 00 00 00 65"},
		{"00 00 00 00 64 00", "00 00 00 00 64 00", "00 00 00 00 64 01"},
		{"00 00 00 00 64 7f", "00 00 00 00 64 7f", "00 00 00 00 64 8001"},
		{"00 00 00 00 64 80", "00 00 00 00 64 8001", "00 00 00 00 64 81ffffffffffffff7f"},
		{"00 00 00 00 64 ff01", "00 00 00 00 64 ff01", "00 00 00 00 64 8002"},
		{"00 00 00 00 64 ffff", "00 00 00 00 64 ffff01", "00 00 00 00 65"},
		{"00 00 00 00 64 ffffffffffffffff7f", "00 00 00 00 64 ffffffffffffffff7f", "00 00 00 00 65"},
		{"00 00 00 00 64 ffffffffffffffffff", "00 00 00 00 64", "00 00 00 00 65"},
		{"00 00 00 00 c9", "00 00 00 00 c9", "00 00 00 00 ca"},
		{"00 00 00 00 c9 00", "00 00 00 00 c9 00", "00 00 00 00 c9 01"},
		{"00 00 00 00 c9 00 00", "00 00 00 00 c9 00 00", "00 00 00 00 c9 00 01"},
		{"00 00 00 00 c9 01", "00 00 00 00 c9 01", "00 00 00 00 c9 02"},
		{"00 00 00 00 c9 01 00", "00 00 00 00 c9 01 00", "00 00 00 00 c9 01 01"},
		{"00 00 00 00 c9 01 ff", "00 00 00 00 c9 01 ff", "00 00 00 00 c9 02"},
		{"00 00 00 00 c9 01 0000", "00 00 00 00 c9 01 0000", "00 00 00 00 c9 01 0001"},
		{"00 00 00 00 c9 01 ffff", "00 00 00 00 c9 01 ffff", "00 00 00 00 c9 02"},
		{"00 00 00 00 c9 01 ffff 00", "00 00 00 00 c9 01 ffff 00", "00 00 00 00 c9 01 ffff 01"},
		{"00 00 00 00 c9 01 ffff 01 0000", "00 00 00 00 c9 01 ffff 01 0000", "00 00 00 00 c9 01 ffff 01 0001"},
		{"00 00 00 00 c9 01 ffff 01 ffff", "00 00 00 00 c9 01 ffff 01 ffff", "00 00 00 00 c9 01 ffff 02"},
		{"00 00 00 00 c9 80", "00 00 00 00 c9 8001", "00 00 00 00 c9 81ffffffffffffff7f"},
		{"00 00 00 00 c9 ffffffffffffffff7f", "00 00 00 00 c9 ffffffffffffffff7f", "00 00 00 00 ca"},
		{"00 00 00 00 c9 ffffffffffffffff7f ffff", "00 00 00 00 c9 ffffffffffffffff7f ffff", "00 00 00 00 ca"},

		{"00 01 00 00 00", "00 01 00 00 00", "00 01 00 00 01"},
		{"00 01 00 00 32 00", "00 01 00 00 32 00", "00 01 00 00 32 01"},
		{"00 01 00 00 32 00 00", "00 01 00 00 32 00 00", "00 01 00 00 32 00 01"},
		{"00 01 00 00 32 00 ff", "00 01 00 00 32 00 ff", "00 01 00 00 32 01"},
		{"00 01 00 00 32 ffffffffffffffff7f ff", "00 01 00 00 32 ffffffffffffffff7f ff", "00 01 00 00 33"},
		{"00 01 00 00 64 00 00 00", "00 01 00 00 64 00 00 00", "00 01 00 00 64 00 00 01"},
		{"00 01 00 00 64 ffffffffffffffff7f ffffffffffffffff7f ff", "00 01 00 00 64 ffffffffffffffff7f ffffffffffffffff7f ff", "00 01 00 00 65"},
		{"00 01 00 00 96 00", "00 01 00 00 96 00", "00 01 00 00 96 01"},
		{"00 01 00 00 97 00 00", "00 01 00 00 97 00 00", "00 01 00 00 97 00 01"},
		{"00 01 00 00 97 ffffffffffffffff7f ffffffffffffffff7f", "00 01 00 00 97 ffffffffffffffff7f ffffffffffffffff7f", "00 01 00 00 98"},
		{"00 01 00 00 c8 01 ffff", "00 01 00 00 c8 01 ffff", "00 01 00 00 c8 02"},
		{"00 01 00 00 c9 00 00", "00 01 00 00 c9 00 00", "00 01 00 00 c9 00 01"},

		{"00 01 01 00 0123", "00 01 01 00", "00 01 01 01"},
		{"00 01 01 01 00", "00 01 01 01 00", "00 01 01 01 04"},
		{"00 01 01 01 04", "00 01 01 01 04", "00 01 01 01 06"},
		{"00 01 01 01 04 00", "00 01 01 01 04 00", "00 01 01 01 04 01"},
		{
			"00 01 01 01 04 07 04 06 00 0600 0100 020000000000000000 030000000000000000 05 00 0600 0100 020000000000000000 030000000000000000 05",
			"00 01 01 01 04 07 04 06 00 0600 0100 020000000000000000 030000000000000000 05 00 0600 0100 020000000000000000 030000000000000000 05",
			"00 01 01 01 04 07 04 06 00 0600 0100 020000000000000000 030000000000000000 05 00 0600 0100 020000000000000000 05",
		},
		{"00 01 01 01 04 80", "00 01 01 01 04 8001", "00 01 01 01 04 81ffffffffffffff7f"},
		{"00 01 01 01 06", "00 01 01 01 06", "00 01 01 01 01"},
		{"00 01 01 01 06 00", "00 01 01 01 06 00", "00 01 01 01 06 01"},
		{"00 01 01 01 06 02 0123", "00 01 01 01 06 02 0123", "00 01 01 01 06 02 0124"},
		{"00 01 01 01 06 02 ffff", "00 01 01 01 06 02 ffff", "00 01 01 01 06 03"},
		{"00 01 01 01 06 80", "00 01 01 01 06 8001", "00 01 01 01 06 81ffffffffffffff7f"},
		{"00 01 01 01 06 ffffffffffffffff7f", "00 01 01 01 06 ffffffffffffffff7f", "00 01 01 01 01"},
		{"00 01 01 01 01", "00 01 01 01 01", "00 01 01 01 02"},
		{"00 01 01 01 01 00", "00 01 01 01 01 00", "00 01 01 01 01 01"},
		{"00 01 01 01 01 01 0123", "00 01 01 01 01 01 0123", "00 01 01 01 01 01 0124"},
		{"00 01 01 01 01 01 ffff", "00 01 01 01 01 01 ffff", "00 01 01 01 01 02"},
		{"00 01 01 01 01 80", "00 01 01 01 01 8001", "00 01 01 01 01 81ffffffffffffff7f"},
		{"00 01 01 01 01 ffffffffffffffff7f", "00 01 01 01 01 ffffffffffffffff7f", "00 01 01 01 02"},
		{"00 01 01 01 02", "00 01 01 01 02", "00 01 01 01 03"},
		{"00 01 01 01 02 ffff", "00 01 01 01 02", "00 01 01 01 03"},
		{"00 01 01 01 02 182d4454fb210940", "00 01 01 01 02 182d4454fb210940", "00 01 01 01 03"},
		{"00 01 01 01 03", "00 01 01 01 03", "00 01 01 01 05"},
		{"00 01 01 01 03 ffff", "00 01 01 01 03", "00 01 01 01 05"},
		{"00 01 01 01 03 182d4454fb210940", "00 01 01 01 03 182d4454fb210940", "00 01 01 01 05"},
		{"00 01 01 01 05", "00 01 01 01 05", "00 01 01 02"},
		{"00 01 01 01 07", "00 01 01 01", "00 01 01 02"},
		{"00 01 01 02 05", "00 01 01 02 05", "00 01 01 03"},
		{"00 01 01 03 05", "00 01 01 03 05", "00 01 01 04"},
		{"ff ffffffffffffff7f ffffffffffffff7f ffffffff 05", "ff ffffffffffffff7f ffffffffffffff7f ffffffff 05", ""},
	}

	for _, tc := range cases {
		prefix := decodeHex(tc.Prefix)
		start := decodeHex(tc.Start)
		limit := decodeHex(tc.Limit)
		slice := Prefix(prefix)
		if slice == nil {
			slice = &util.Range{}
		}

		if !bytes.Equal(slice.Start, start) {
			t.Errorf(`Start("%s") expects "%s" but got "%x"`, tc.Prefix, tc.Start, slice.Start)
		}
		if !bytes.Equal(slice.Limit, limit) {
			t.Errorf(`Limit("%s") expects "%s" but got "%x"`, tc.Prefix, tc.Limit, slice.Limit)
		}
		if slice.Limit != nil && Comparer.Compare(slice.Start, slice.Limit) > 0 {
			t.Errorf(`Start("%s") is greater than Limit("%s")`, tc.Prefix, tc.Prefix)
			t.Logf(`Start("%s") == "%x"`, tc.Prefix, slice.Start)
			t.Logf(`Limit("%s") == "%x"`, tc.Prefix, slice.Limit)
		}
	}

	keys := []string{
		"00 00 00 00 00",
		"00 00 00 00 01",
		"00 00 00 00 02",
		"00 00 00 00 64 8001",
		"00 00 00 00 c9 04 0074006500730074 04 0074006500730074",
		"00 01 00 00 00",
		"00 01 00 00 01",
		"00 01 00 00 32 8001 00",
		"00 01 00 00 64 8001 8001 00",
		"00 01 00 00 96 8001",
		"00 01 00 00 97 8001 8001",
		"00 01 00 00 c8 04 0074006500730074",
		"00 01 00 00 c9 8001 04 0074006500730074",
		"00 01 00 00 c9 8001 04 0074006500730074",
		"00 01 01 01 00",
		"00 01 01 01 04 01 00",
		"00 01 01 02 06 04 74657374",
		"00 01 01 02 01 04 0074006500730074",
		"00 01 01 03 02 0000000000000000",
		"00 01 01 20 03 0000000000000000",
	}

	for _, keyString := range keys {
		key := decodeHex(keyString)

		for i := 1; i <= len(key); i++ {
			prefix := key[:i]
			slice := Prefix(prefix)

			if slice.Start != nil && Comparer.Compare(slice.Start, key) > 0 {
				t.Errorf(`Start("%x") is greater than "%s"`, prefix, keyString)
				t.Logf(`Start("%x") == "%x"`, prefix, slice.Start)
			}
			if slice.Limit != nil && Comparer.Compare(slice.Limit, key) <= 0 {
				t.Errorf(`Limit("%x") is less than or equals to "%s"`, prefix, keyString)
				t.Logf(`Limit("%x") == "%x"`, prefix, slice.Limit)
			}
			if slice.Limit != nil && Comparer.Compare(slice.Start, slice.Limit) > 0 {
				t.Errorf(`Start("%x") is greater than Limit("%x")`, prefix, prefix)
				t.Logf(`Start("%x") == "%x"`, prefix, slice.Start)
				t.Logf(`Limit("%x") == "%x"`, prefix, slice.Limit)
			}
		}
	}
}
