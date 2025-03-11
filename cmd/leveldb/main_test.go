// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/cions/go-colorterm"
)

func TestLdbFilesPattern(t *testing.T) {
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
		if !ldbFilesPattern.MatchString(filename) {
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
		if ldbFilesPattern.MatchString(filename) {
			t.Errorf("%q should not match", filename)
		}
	}
}

func TestEncodeBase64(t *testing.T) {
	tests := []struct {
		input []byte
		want  string
	}{
		{[]byte(""), ""},
		{[]byte("a"), "YQ=="},
		{[]byte("ab"), "YWI="},
		{[]byte("abc"), "YWJj"},
		{[]byte("abcd"), "YWJjZA=="},
	}
	for _, tt := range tests {
		if got := EncodeBase64(tt.input); got != tt.want {
			t.Errorf("EncodeBase64(%q): expected %q, but got %q", tt.input, tt.want, got)
		}
	}
}

func TestFormatter(t *testing.T) {
	colorterm.Enabled = false

	tests := []struct {
		input   []byte
		want    string
		quoting bool
	}{
		{[]byte(""), ``, false},
		{[]byte(""), `""`, true},
		{[]byte("Hello, 世界！"), `Hello, 世界！`, false},
		{[]byte("Hello, 世界！"), `"Hello, 世界！"`, true},
		{[]byte("\x00\x01\a\b\f\n\r\t\v\"\\"), `\0\x01\a\b\f\n\r\t\v"\\`, false},
		{[]byte("\x00\x01\a\b\f\n\r\t\v\"\\"), `"\0\x01\a\b\f\n\r\t\v\"\\"`, true},
		{[]byte("\x80\u0080\U0001d53a"), `\x80\u0080\U0001D53A`, false},
		{[]byte("\x80\u0080\U0001d53a"), `"\x80\u0080\U0001D53A"`, true},
		{[]byte(`null`), `null`, true},
		{[]byte(`"string"`), `"string"`, true},
		{[]byte(`{"key":"value"}`), "{\n  \"key\": \"value\"\n}", true},
		{[]byte(`"{\"key\":\"value\"}"`), "{\n  \"key\": \"value\"\n}", true},
		{bytes.Repeat([]byte("a\x80"), 50), `"` + strings.Repeat(`a\x80`, 24) + `..."`, true},
	}
	for _, tt := range tests {
		f := NewFormatter(tt.quoting, true, true)
		if got := f(tt.input); got != tt.want {
			t.Errorf("Format(%q): expected %q, but got %q", tt.input, tt.want, got)
		}
	}
}

func TestParseBase64(t *testing.T) {
	tests := []struct {
		input string
		want  []byte
	}{
		{"", []byte("")},
		{"Y", nil},
		{"YQ", []byte("a")},
		{"YWI=", []byte("ab")},
		{"YWJj", []byte("abc")},
		{"YWJjZA", []byte("abcd")},
		{"YWJjZA==", []byte("abcd")},
		{"YWJjZA!!", nil},
	}
	for _, tt := range tests {
		got, err := ParseBase64(tt.input)
		switch {
		case tt.want == nil && err == nil:
			t.Errorf("ParseBase64(%q): expected a non-nil error", tt.input)
		case tt.want != nil && err != nil:
			t.Errorf("ParseBase64(%q): unexpected error: %v", tt.input, err)
		case tt.want != nil && !bytes.Equal(got, tt.want):
			t.Errorf("ParseBase64(%q): expected %q, but got %q", tt.input, tt.want, got)
		}
	}
}

func TestParseEscaped(t *testing.T) {
	tests := []struct {
		input string
		want  []byte
	}{
		{``, []byte{}},
		{`abc`, []byte{'a', 'b', 'c'}},
		{`\0\x01\a\b\f\n\r\t\v\"\\`, []byte{'\x00', '\x01', '\a', '\b', '\f', '\n', '\r', '\t', '\v', '"', '\\'}},
		{`\x80\u0080\U0001d53Aa`, []byte{0x80, 0xc2, 0x80, 0xf0, 0x9d, 0x94, 0xba, 'a'}},
		{`\`, nil},
		{`\x`, nil},
		{`\xXX`, nil},
		{`\u00`, nil},
		{`\uXXXX`, nil},
		{`\U0000`, nil},
		{`\UXXXXXXXX`, nil},
	}
	for _, tt := range tests {
		got, err := ParseEscaped(tt.input)
		switch {
		case tt.want == nil && err == nil:
			t.Errorf("ParseEscaped(%q): expected a non-nil error", tt.input)
		case tt.want != nil && err != nil:
			t.Errorf("ParseEscaped(%q): unexpected error: %v", tt.input, err)
		case tt.want != nil && !bytes.Equal(got, tt.want):
			t.Errorf("ParseEscaped(%q): expected %q, but got %q", tt.input, tt.want, got)
		}
	}
}
