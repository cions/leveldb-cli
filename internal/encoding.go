package internal

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/fatih/color"
)

type base64Writer struct {
	w io.Writer
}

func NewBase64Writer(w io.Writer) *base64Writer {
	return &base64Writer{w: w}
}

func (w *base64Writer) Write(b []byte) (int, error) {
	enc := base64.NewEncoder(base64.StdEncoding, w.w)
	if _, err := enc.Write(b); err != nil {
		return 0, nil
	}
	if err := enc.Close(); err != nil {
		return 0, nil
	}
	return base64.StdEncoding.EncodedLen(len(b)), nil
}

func (w *base64Writer) Unwrap() io.Writer {
	return w.w
}

type quotingWriter struct {
	w           io.Writer
	doubleQuote bool
	parseJSON   bool
	wide        bool
}

func NewQuotingWriter(w io.Writer) *quotingWriter {
	return &quotingWriter{w: w}
}

func (w *quotingWriter) SetDoubleQuote(b bool) *quotingWriter {
	w.doubleQuote = b
	return w
}

func (w *quotingWriter) SetParseJSON(b bool) *quotingWriter {
	w.parseJSON = b
	return w
}

func (w *quotingWriter) SetWide(b bool) *quotingWriter {
	w.wide = b
	return w
}

func (w *quotingWriter) Write(b []byte) (int, error) {
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
	buf.Grow(len(b))
	if w.doubleQuote {
		buf.WriteByte('"')
	}
	for len(b) > 0 {
		if !w.wide && buf.Len() > 100 {
			red(buf, "...")
			break
		}
		r, size := utf8.DecodeRune(b)
		switch {
		case r == utf8.RuneError:
			red(buf, "\\x%02x", b[0])
		case r == 0:
			red(buf, "\\0")
		case r == '"':
			if w.doubleQuote {
				red(buf, "\\\"")
			} else {
				buf.WriteByte(byte(r))
			}
		case r == '\\':
			red(buf, "\\\\")
		case r == '\a':
			red(buf, "\\a")
		case r == '\b':
			red(buf, "\\b")
		case r == '\f':
			red(buf, "\\f")
		case r == '\n':
			red(buf, "\\n")
		case r == '\r':
			red(buf, "\\r")
		case r == '\t':
			red(buf, "\\t")
		case r == '\v':
			red(buf, "\\v")
		case unicode.IsPrint(r):
			buf.WriteRune(r)
		case r < utf8.RuneSelf:
			red(buf, "\\x%02x", r)
		case r <= 0xffff:
			red(buf, "\\u%04x", r)
		default:
			red(buf, "\\U%08x", r)
		}
		b = b[size:]
	}
	if w.doubleQuote {
		buf.WriteByte('"')
	}
	n, err := buf.WriteTo(w.w)
	return int(n), err
}

func (w *quotingWriter) Unwrap() io.Writer {
	return w.w
}

func DecodeBase64(b []byte) ([]byte, error) {
	b = bytes.TrimRight(b, "=")
	n, err := base64.RawStdEncoding.Decode(b, b)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}

func parseHex(b []byte, n int) (uint, bool) {
	if len(b) < n {
		return 0, false
	}
	var x uint
	for i := 0; i < n; i++ {
		x <<= 4
		switch {
		case '0' <= b[i] && b[i] <= '9':
			x |= uint(b[i] - '0')
		case 'A' <= b[i] && b[i] <= 'F':
			x |= uint(b[i] - 'A' + 10)
		case 'a' <= b[i] && b[i] <= 'f':
			x |= uint(b[i] - 'a' + 10)
		default:
			return 0, false
		}
	}
	return x, true
}

func Unquote(b []byte) ([]byte, error) {
	s, d := 0, 0
	for s < len(b) {
		if b[s] != '\\' {
			b[d] = b[s]
			s++
			d++
			continue
		}
		s++
		if s == len(b) {
			return nil, fmt.Errorf("truncated backslash escape at position %d", s-1)
		}
		ssize := 1
		dsize := 1
		switch b[s] {
		case '0':
			b[d] = '\x00'
		case 'a':
			b[d] = '\a'
		case 'b':
			b[d] = '\b'
		case 'f':
			b[d] = '\f'
		case 'n':
			b[d] = '\n'
		case 'r':
			b[d] = '\r'
		case 't':
			b[d] = '\t'
		case 'v':
			b[d] = '\v'
		case 'x':
			s++
			r, ok := parseHex(b[s:], 2)
			if !ok {
				return nil, fmt.Errorf("truncated \\x escape at position %d", s-2)
			}
			b[d] = byte(r)
			ssize = 2
		case 'u':
			s++
			r, ok := parseHex(b[s:], 4)
			if !ok {
				return nil, fmt.Errorf("truncated \\u escape at position %d", s-2)
			}
			ssize = 4
			dsize = utf8.EncodeRune(b[d:], rune(r))
		case 'U':
			s++
			r, ok := parseHex(b[s:], 8)
			if !ok {
				return nil, fmt.Errorf("truncated \\U escape at position %d", s-2)
			}
			ssize = 8
			dsize = utf8.EncodeRune(b[d:], rune(r))
		default:
			b[d] = b[s]
		}
		s += ssize
		d += dsize
	}
	return b[:d], nil
}
