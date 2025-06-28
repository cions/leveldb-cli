// Copyright (c) 2021-2025 cions
// Licensed under the MIT License. See LICENSE for details.

package main

import (
	"bytes"
	"io"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/vmihailenco/msgpack/v5"
)

type DumpFormat int

const (
	MessagePackStream DumpFormat = iota
	MessagePack
)

type DumpFileEncoder interface {
	Encode(key, value []byte) error
	Close() error
}

type DumpFileDecoder interface {
	Decode(batch *leveldb.Batch, batchLimit int) error
}

type MessagePackEncoder struct {
	encoder *msgpack.Encoder
	entries []Entry
}

func NewMessagePackEncoder(w io.Writer) *MessagePackEncoder {
	encoder := msgpack.NewEncoder(w)
	encoder.UseCompactInts(true)
	return &MessagePackEncoder{encoder, nil}
}

func (e *MessagePackEncoder) Encode(key, value []byte) error {
	e.entries = append(e.entries, Entry{
		Key:   bytes.Clone(key),
		Value: bytes.Clone(value),
	})
	return nil
}

func (e *MessagePackEncoder) Close() error {
	if err := e.encoder.EncodeMapLen(len(e.entries)); err != nil {
		return err
	}
	for _, entry := range e.entries {
		if err := e.encoder.EncodeBytes(entry.Key); err != nil {
			return err
		}
		if err := e.encoder.EncodeBytes(entry.Value); err != nil {
			return err
		}
	}
	return nil
}

type MessagePackStreamEncoder struct {
	encoder *msgpack.Encoder
}

func NewMessagePackStreamEncoder(w io.Writer) *MessagePackStreamEncoder {
	encoder := msgpack.NewEncoder(w)
	encoder.UseCompactInts(true)
	return &MessagePackStreamEncoder{encoder}
}

func (e *MessagePackStreamEncoder) Encode(key, value []byte) error {
	if err := e.encoder.EncodeBytes(key); err != nil {
		return err
	}
	if err := e.encoder.EncodeBytes(value); err != nil {
		return err
	}
	return nil
}

func (e *MessagePackStreamEncoder) Close() error {
	return nil
}

type MessagePackDecoder struct {
	decoder  *msgpack.Decoder
	nentries int
}

func NewMessagePackDecoder(r io.Reader) (*MessagePackDecoder, error) {
	decoder := msgpack.NewDecoder(r)
	nentries, err := decoder.DecodeMapLen()
	if err != nil {
		return nil, err
	}
	return &MessagePackDecoder{decoder, nentries}, nil
}

func (d *MessagePackDecoder) Decode(batch *leveldb.Batch, batchLimit int) error {
	count := d.nentries
	if batchLimit > 0 {
		count = min(count, batchLimit)
	}
	for range count {
		key, err := d.decoder.DecodeBytes()
		if err != nil {
			return err
		}
		value, err := d.decoder.DecodeBytes()
		if err != nil {
			return err
		}
		batch.Put(key, value)
	}
	d.nentries -= count
	return nil
}

type MessagePackStreamDecoder struct {
	decoder *msgpack.Decoder
}

func NewMessagePackStreamDecoder(r io.Reader) (*MessagePackStreamDecoder, error) {
	decoder := msgpack.NewDecoder(r)
	return &MessagePackStreamDecoder{decoder}, nil
}

func (d *MessagePackStreamDecoder) Decode(batch *leveldb.Batch, batchLimit int) error {
	for {
		key, err := d.decoder.DecodeBytes()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		value, err := d.decoder.DecodeBytes()
		if err != nil {
			return err
		}
		batch.Put(key, value)
		if uint(batch.Len()) > uint(batchLimit)-1 {
			break
		}
	}
	return nil
}
