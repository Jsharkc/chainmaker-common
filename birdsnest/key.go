/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"encoding/hex"
	"errors"
)

var (
	SeparatorString = "-"
	// Separator chainmaker ca
	Separator = byte(202)

	ErrKeyLengthCannotBeZero = errors.New("the key length cannot be 0")
	ErrNotTimestampKey       = errors.New("not timestamp txid")
	ErrTimestampKeyIsInvalid = errors.New("TxId nanosecond is invalid")
)

type Key interface {
	// Parse the key
	Parse() ([][]byte, error)
	// Key bytes
	Key() []byte
	// Len The length of the key
	Len() int
	String() string
	GetNano() int64
}

// TimestampKey Converting TxId directly using TimestampKey is not allowed, see ToTimestampKey
type TimestampKey []byte

func ToTimestampKey(txId string) (TimestampKey, error) {
	b, err := hex.DecodeString(txId)
	if err != nil {
		return nil, err
	}
	if b[8] != Separator {
		return nil, ErrNotTimestampKey
	}
	if bytes2nano(b[:8]) < 0 {
		return nil, ErrTimestampKeyIsInvalid
	}
	key := TimestampKey(b)
	return key, nil
}

// ToStrings TimestampKey to string
func ToStrings(keys []Key) []string {
	result := make([]string, len(keys))
	for i := range keys {
		result[i] = keys[i].String()
	}
	return result
}

func (k TimestampKey) Len() int {
	return len(k)
}

func (k TimestampKey) Key() []byte {
	return k
}

func (k TimestampKey) String() string {
	return hex.EncodeToString(k)
}

func (k TimestampKey) GetNano() int64 {
	return bytes2nano(k[:8])
}

func (k TimestampKey) Parse() ([][]byte, error) {
	if len(k) == 0 {
		return nil, ErrKeyLengthCannotBeZero
	}
	if k[8] != Separator {
		return nil, ErrNotTimestampKey
	}
	if k.GetNano() < 0 {
		return nil, ErrTimestampKeyIsInvalid
	}
	return [][]byte{k[:8], k[8:32]}, nil
}
