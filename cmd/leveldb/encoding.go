// Copyright (c) 2021-2023 cions
// Licensed under the MIT License. See LICENSE for details.

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"unicode"
	"unicode/utf8"

	"github.com/fatih/color"
)

func init() {
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		color.NoColor = true
	}
}

type base64Writer struct {
	w io.Writer
}

func newBase64Writer(w io.Writer) *base64Writer {
	return &base64Writer{w}
}

func (w *base64Writer) Write(b []byte) (int, error) {
	enc := base64.NewEncoder(base64.StdEncoding, w.w)
	if _, err := enc.Write(b); err != nil {
		return 0, err
	}
	if err := enc.Close(); err != nil {
		return 0, err
	}
	return base64.StdEncoding.EncodedLen(len(b)), nil
}

type prettyPrinter struct {
	w         io.Writer
	quoting   bool
	truncate  bool
	parseJSON bool
}

func newPrettyPrinter(w io.Writer) *prettyPrinter {
	return &prettyPrinter{w: w}
}

func (w *prettyPrinter) SetQuoting(b bool) *prettyPrinter {
	w.quoting = b
	return w
}

func (w *prettyPrinter) SetTruncate(b bool) *prettyPrinter {
	w.truncate = b
	return w
}

func (w *prettyPrinter) SetParseJSON(b bool) *prettyPrinter {
	w.parseJSON = b
	return w
}

func (w *prettyPrinter) Write(b []byte) (int, error) {
	red := color.New(color.FgRed).FprintfFunc()

	if w.parseJSON {
		for {
			var s *string
			if err := json.Unmarshal(b, &s); err != nil || s == nil {
				break
			}
			b = []byte(*s)
		}

		var obj interface{}
		if err := json.Unmarshal(b, &obj); err == nil {
			buf := new(bytes.Buffer)
			enc := json.NewEncoder(buf)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			if err := enc.Encode(obj); err != nil {
				return 0, err
			}
			buf.Truncate(buf.Len() - 1)
			n, err := buf.WriteTo(w.w)
			return int(n), err
		}
	}

	buf := new(bytes.Buffer)
	if !w.truncate {
		buf.Grow(len(b))
	}
	if w.quoting {
		buf.WriteByte('"')
	}
	nwritten := 0
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		switch {
		case r == utf8.RuneError:
			red(buf, "\\x%02x", b[0])
			nwritten += 4
		case r == 0:
			red(buf, "\\0")
			nwritten += 2
		case r == '"' && w.quoting:
			red(buf, "\\\"")
			nwritten += 2
		case r == '\\':
			red(buf, "\\\\")
			nwritten += 2
		case r == '\a':
			red(buf, "\\a")
			nwritten += 2
		case r == '\b':
			red(buf, "\\b")
			nwritten += 2
		case r == '\f':
			red(buf, "\\f")
			nwritten += 2
		case r == '\n':
			red(buf, "\\n")
			nwritten += 2
		case r == '\r':
			red(buf, "\\r")
			nwritten += 2
		case r == '\t':
			red(buf, "\\t")
			nwritten += 2
		case r == '\v':
			red(buf, "\\v")
			nwritten += 2
		case unicode.IsPrint(r):
			buf.WriteRune(r)
			nwritten += 1
		case r <= 0x7f:
			red(buf, "\\x%02x", r)
			nwritten += 4
		case r <= 0xffff:
			red(buf, "\\u%04x", r)
			nwritten += 6
		default:
			red(buf, "\\U%08x", r)
			nwritten += 8
		}
		b = b[size:]
		if w.truncate && nwritten >= 250 {
			red(buf, "...")
			break
		}
	}
	if w.quoting {
		buf.WriteByte('"')
	}
	n, err := buf.WriteTo(w.w)
	return int(n), err
}

func decodeBase64(b []byte) ([]byte, error) {
	b = bytes.TrimRight(b, "=")
	n, err := base64.RawStdEncoding.Decode(b, b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}

func parseHex(b []byte, n int) (uint32, bool) {
	if len(b) < n {
		return 0, false
	}
	x := uint32(0)
	for i := 0; i < n; i++ {
		x <<= 4
		switch {
		case '0' <= b[i] && b[i] <= '9':
			x |= uint32(b[i] - '0')
		case 'A' <= b[i] && b[i] <= 'F':
			x |= uint32(b[i] - 'A' + 10)
		case 'a' <= b[i] && b[i] <= 'f':
			x |= uint32(b[i] - 'a' + 10)
		default:
			return 0, false
		}
	}
	return x, true
}

func unescape(b []byte) ([]byte, error) {
	dst := b[:0]
	i := 0
	for i < len(b) {
		if b[i] != '\\' {
			dst = append(dst, b[i])
			i += 1
			continue
		}
		if i+1 == len(b) {
			return nil, fmt.Errorf("truncated backslash escape at position %d", i)
		}
		advance := 2
		switch b[i+1] {
		case '0':
			dst = append(dst, '\x00')
		case 'a':
			dst = append(dst, '\a')
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'v':
			dst = append(dst, '\v')
		case 'x':
			cp, ok := parseHex(b[i+2:], 2)
			if !ok {
				return nil, fmt.Errorf("truncated \\x escape at position %d", i)
			}
			dst = append(dst, byte(cp))
			advance = 4
		case 'u':
			cp, ok := parseHex(b[i+2:], 4)
			if !ok {
				return nil, fmt.Errorf("truncated \\u escape at position %d", i)
			}
			dst = utf8.AppendRune(dst, rune(cp))
			advance = 6
		case 'U':
			cp, ok := parseHex(b[i+2:], 8)
			if !ok {
				return nil, fmt.Errorf("truncated \\U escape at position %d", i)
			}
			dst = utf8.AppendRune(dst, rune(cp))
			advance = 10
		default:
			dst = append(dst, b[i+1])
		}
		i += advance
	}
	return dst, nil
}
