package leveldbcli

import (
	"bytes"
	"testing"

	"github.com/fatih/color"
)

func TestBase64Writer(t *testing.T) {
	cases := []struct {
		input, want []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("abc"), []byte("YWJj")},
		{[]byte("abcd"), []byte("YWJjZA==")},
	}

	buf := new(bytes.Buffer)
	w := newBase64Writer(buf)
	for _, tc := range cases {
		buf.Reset()
		n, err := w.Write(tc.input)
		if err != nil {
			t.Errorf("Write(%q): unexpected error: %v", tc.input, err)
		} else if !bytes.Equal(buf.Bytes(), tc.want) {
			t.Errorf("Write(%q) = %q, want %q", tc.input, buf.Bytes(), tc.want)
		} else if n != len(tc.want) {
			t.Errorf("Write(%q) returns %d, want %d", tc.input, n, len(tc.want))
		}
	}
}

func TestPrettyPrinter(t *testing.T) {
	cases := []struct {
		input, want                  []byte
		quoting, truncate, parseJSON bool
	}{
		{[]byte(""), []byte(``), false, false, false},
		{[]byte(""), []byte(`""`), true, false, false},
		{[]byte("Hello, 世界！"), []byte(`Hello, 世界！`), false, false, false},
		{[]byte("Hello, 世界！"), []byte(`"Hello, 世界！"`), true, false, false},
		{[]byte("\"\x00\x01\a\b\f\n\r\t\v\\\""), []byte(`"\0\x01\a\b\f\n\r\t\v\\"`), false, false, false},
		{[]byte("\"\x00\x01\a\b\f\n\r\t\v\\\""), []byte(`"\"\0\x01\a\b\f\n\r\t\v\\\""`), true, false, false},
		{[]byte("\x80\u0080\U0001d53a"), []byte(`\x80\u0080\U0001d53a`), false, false, false},
		{[]byte("\x80\u0080\U0001d53a"), []byte(`"\x80\u0080\U0001d53a"`), true, false, false},
		{[]byte(`null`), []byte(`null`), false, false, true},
		{[]byte(`"string"`), []byte(`string`), false, false, true},
		{[]byte(`{"key":"value"}`), []byte("{\n  \"key\": \"value\"\n}"), false, false, true},
		{[]byte(`"{\"key\":\"value\"}"`), []byte("{\n  \"key\": \"value\"\n}"), false, false, true},
		{bytes.Repeat([]byte("a\x80"), 100), bytes.Repeat([]byte(`a\x80`), 100), false, false, false},
		{bytes.Repeat([]byte("a\x80"), 100), append(bytes.Repeat([]byte(`a\x80`), 50), '.', '.', '.'), false, true, false},
	}

	color.NoColor = true
	buf := new(bytes.Buffer)
	w := newPrettyPrinter(buf)
	for _, tc := range cases {
		buf.Reset()
		w.SetQuoting(tc.quoting)
		w.SetTruncate(tc.truncate)
		w.SetParseJSON(tc.parseJSON)
		n, err := w.Write(tc.input)
		if err != nil {
			t.Errorf("Write(%q): unexpected error: %v", tc.input, err)
		} else if !bytes.Equal(buf.Bytes(), tc.want) {
			t.Errorf("Write(%q) = %q, want %q", tc.input, buf.Bytes(), tc.want)
		} else if n != len(tc.want) {
			t.Errorf("Write(%q) returns %d, want %d", tc.input, n, len(tc.want))
		}
	}
}

func TestDecodeBase64(t *testing.T) {
	cases := []struct {
		input, want []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("Y"), nil},
		{[]byte("YW"), []byte("a")},
		{[]byte("YWJ"), []byte("ab")},
		{[]byte("YWJj"), []byte("abc")},
		{[]byte("YWJjZA"), []byte("abcd")},
		{[]byte("YWJjZA=="), []byte("abcd")},
		{[]byte("YWJjZA@@"), nil},
	}

	for _, tc := range cases {
		got, err := decodeBase64(tc.input)
		if tc.want == nil && err == nil {
			t.Errorf("decodeBase64(%q) should fail", tc.input)
		} else if tc.want != nil && err != nil {
			t.Errorf("decodeBase64(%q): unexpected error: %v", tc.input, err)
		} else if tc.want != nil && !bytes.Equal(got, tc.want) {
			t.Errorf("decodeBase64(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestUnescape(t *testing.T) {
	cases := []struct {
		input, want []byte
	}{
		{[]byte(``), []byte{}},
		{[]byte(`abc`), []byte{'a', 'b', 'c'}},
		{[]byte(`\\\"\0\x01\a\b\f\n\r\t\v`), []byte{'\\', '"', 0, 1, '\a', '\b', '\f', '\n', '\r', '\t', '\v'}},
		{[]byte(`\x80\u0080\U0001d53Aa`), []byte{0x80, 0xc2, 0x80, 0xf0, 0x9d, 0x94, 0xba, 'a'}},
		{[]byte(`\`), nil},
		{[]byte(`\x`), nil},
		{[]byte(`\xXX`), nil},
		{[]byte(`\uXXXX`), nil},
		{[]byte(`\UXXXXXXXX`), nil},
	}

	for _, tc := range cases {
		got, err := unescape(tc.input)
		if tc.want == nil && err == nil {
			t.Errorf("unescape(%q) should fail", tc.input)
		} else if tc.want != nil && err != nil {
			t.Errorf("unescape(%q): unexpected error: %v", tc.input, err)
		} else if tc.want != nil && !bytes.Equal(got, tc.want) {
			t.Errorf("unescape(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
