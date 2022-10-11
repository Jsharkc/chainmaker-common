/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"encoding/binary"
	"errors"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"go.uber.org/atomic"
)

var (

	// ErrKeyTimeIsNotInTheFilterRange Not error; Key time is not in the filter range
	ErrKeyTimeIsNotInTheFilterRange = errors.New("key time is not in the filter range")
)

func ExtensionDeserialize(bytes []byte) (FilterExtension, error) {
	extensionType := common.FilterExtensionType(binary.BigEndian.Uint64(bytes[:8]))
	switch extensionType {
	case common.FilterExtensionType_FETDefault:
		return DeserializeDefault(), nil
	case common.FilterExtensionType_FETTimestamp:
		return DeserializeTimestamp(bytes)
	default:
		return nil, ErrFilterExtensionNotSupport.Error(extensionType)
	}
}

type DefaultFilterExtension struct {
}

func NewDefaultFilterExtension() *DefaultFilterExtension {
	return &DefaultFilterExtension{}
}

func (d DefaultFilterExtension) Validate(Key, bool) error {
	return nil
}

func (d DefaultFilterExtension) Store(Key) error {
	return nil
}

func (d DefaultFilterExtension) Serialize() []byte {
	var type0 = make([]byte, 8)
	binary.BigEndian.PutUint64(type0, uint64(common.FilterExtensionType_FETDefault))

	return type0
}

func DeserializeDefault() FilterExtension {
	return &DefaultFilterExtension{}
}

type TimestampFilterExtension struct {
	firstTimestamp *atomic.Int64
	lastTimestamp  *atomic.Int64
}

func NewTimestampFilterExtension() FilterExtension {
	return &TimestampFilterExtension{
		firstTimestamp: atomic.NewInt64(0),
		lastTimestamp:  atomic.NewInt64(0),
	}
}

func (t *TimestampFilterExtension) Serialize() []byte {
	var type0 = make([]byte, 8)
	binary.BigEndian.PutUint64(type0, uint64(common.FilterExtensionType_FETTimestamp))

	var first = make([]byte, 8)
	binary.BigEndian.PutUint64(first, uint64(t.firstTimestamp.Load()))

	var last = make([]byte, 8)
	binary.BigEndian.PutUint64(last, uint64(t.lastTimestamp.Load()))

	var result []byte
	result = append(result, type0...)
	result = append(result, first...)
	result = append(result, last...)
	return result
}

func (t *TimestampFilterExtension) Validate(key Key, full bool) error {
	nano := key.GetNano()
	if full {
		first := t.firstTimestamp.Load()
		if first != 0 {
			if nano < first {
				return ErrKeyTimeIsNotInTheFilterRange
			}
			if nano > t.lastTimestamp.Load() {
				return ErrKeyTimeIsNotInTheFilterRange
			}
		}
	}
	return nil
}

func (t *TimestampFilterExtension) Store(key Key) error {
	split, err := key.Parse()
	if err != nil {
		return err
	}
	nano := int64(binary.LittleEndian.Uint64(split[0]))
	//timestamp := nano / time.Millisecond.Nanoseconds()
	if t.firstTimestamp.Load() == 0 {
		t.firstTimestamp.Store(nano)
	}
	if nano < t.firstTimestamp.Load() {
		t.firstTimestamp.Store(nano)
	}
	if nano > t.lastTimestamp.Load() {
		t.lastTimestamp.Store(nano)
	}
	return nil
}

func DeserializeTimestamp(bytes []byte) (*TimestampFilterExtension, error) {
	t := &TimestampFilterExtension{
		firstTimestamp: atomic.NewInt64(0),
		lastTimestamp:  atomic.NewInt64(0),
	}
	if len(bytes) != 24 {
		return nil, ErrKeyCannotBeEmpty
	}

	t.firstTimestamp.Store(int64(binary.BigEndian.Uint64(bytes[8:16])))
	t.lastTimestamp.Store(int64(binary.BigEndian.Uint64(bytes[16:24])))
	return t, nil
}
