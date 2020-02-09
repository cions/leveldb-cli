package internal

import (
	"bytes"
	"testing"

	"github.com/fatih/color"
)

func TestBase64Writer(t *testing.T) {
	cases := []struct {
		arg, want []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("abc"), []byte("YWJj")},
		{[]byte("abcd"), []byte("YWJjZA==")},
	}

	buf := new(bytes.Buffer)
	w := NewBase64Writer(buf)
	for i, tc := range cases {
		buf.Reset()
		n, err := w.Write(tc.arg)
		if err != nil {
			t.Errorf("%d: unexpected error: %v\n", i, err)
		} else if !bytes.Equal(buf.Bytes(), tc.want) {
			t.Errorf("%d: expect %s, but got %s\n", i, tc.want, buf.Bytes())
		} else if n != len(tc.want) {
			t.Errorf("%d: expect %d, but got %d\n", i, len(tc.want), n)
		}
	}
}

func TestQuotingWriter(t *testing.T) {
	cases := []struct {
		arg, want                    []byte
		doubleQuote, parseJSON, wide bool
	}{
		{[]byte(""), []byte(``), false, false, false},
		{[]byte(""), []byte(`""`), true, false, false},
		{[]byte("Hello, 世界！"), []byte(`Hello, 世界！`), false, false, false},
		{[]byte("Hello, 世界！"), []byte(`"Hello, 世界！"`), true, false, false},
		{[]byte("\"\x00\a\b\f\n\r\t\v\\\""), []byte(`"\0\a\b\f\n\r\t\v\\"`), false, false, false},
		{[]byte("\"\x00\a\b\f\n\r\t\v\\\""), []byte(`"\"\0\a\b\f\n\r\t\v\\\""`), true, false, false},
		{[]byte("\x80\u0080\U0001d53a"), []byte(`\x80\u0080\U0001d53a`), false, false, false},
		{[]byte("\x80\u0080\U0001d53a"), []byte(`"\x80\u0080\U0001d53a"`), true, false, false},
		{[]byte(`null`), []byte(`null`), false, true, false},
		{[]byte(`"string"`), []byte(`string`), false, true, false},
		{[]byte(`{"key":"value"}`), []byte("{\n  \"key\": \"value\"\n}"), false, true, false},
		{[]byte(`"{\"key\":\"value\"}"`), []byte("{\n  \"key\": \"value\"\n}"), false, true, false},
		{bytes.Repeat([]byte("a\x80"), 50), append(bytes.Repeat([]byte(`a\x80`), 20), 'a', '.', '.', '.'), false, false, false},
		{bytes.Repeat([]byte("a\x80"), 50), bytes.Repeat([]byte(`a\x80`), 50), false, false, true},
	}

	color.NoColor = true
	buf := new(bytes.Buffer)
	w := NewQuotingWriter(buf)
	for i, tc := range cases {
		buf.Reset()
		w.SetDoubleQuote(tc.doubleQuote)
		w.SetParseJSON(tc.parseJSON)
		w.SetWide(tc.wide)
		n, err := w.Write(tc.arg)
		if err != nil {
			t.Errorf("%d: unexpected error: %v\n", i, err)
		} else if !bytes.Equal(buf.Bytes(), tc.want) {
			t.Errorf("%d: expect %s, but got %s\n", i, tc.want, buf.Bytes())
		} else if n != len(tc.want) {
			t.Errorf("%d: expect %d, but got %d\n", i, len(tc.want), n)
		}
	}
}

func TestDecodeBase64(t *testing.T) {
	cases := []struct {
		arg, want []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("YWJj"), []byte("abc")},
		{[]byte("YWJjZA"), []byte("abcd")},
		{[]byte("YWJjZA=="), []byte("abcd")},
	}

	for i, tc := range cases {
		got, err := DecodeBase64(tc.arg)
		if err != nil {
			t.Errorf("%d: unexpected error: %v\n", i, err)
		} else if !bytes.Equal(got, tc.want) {
			t.Errorf("%d: expect %s, but got %s\n", i, tc.want, got)
		}
	}
}

func TestUnquote(t *testing.T) {
	cases := []struct {
		arg, want []byte
	}{
		{[]byte(``), []byte{}},
		{[]byte(`abc`), []byte{'a', 'b', 'c'}},
		{[]byte(`\\\"\0\a\b\f\n\r\t\v`), []byte{'\\', '"', 0, '\a', '\b', '\f', '\n', '\r', '\t', '\v'}},
		{[]byte(`a\x80\u0080\U0001d53Ab`), []byte{'a', 0x80, 0xc2, 0x80, 0xf0, 0x9d, 0x94, 0xba, 'b'}},
	}

	color.NoColor = true
	for i, tc := range cases {
		got, err := Unquote(tc.arg)
		if err != nil {
			t.Errorf("%d: unexpected error: %v\n", i, err)
		} else if !bytes.Equal(got, tc.want) {
			t.Errorf("%d: want %v, but got %v\n", i, tc.want, got)
		}
	}
}
