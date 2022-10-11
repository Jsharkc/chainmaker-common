/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"encoding/binary"
	"testing"
	"time"
)

// ToTimestampKeysAndNormalKeys string to TimestampKey return timestampKeys and normalKeys
func ToTimestampKeysAndNormalKeys(key []string) (timestampKeys []Key, normalKeys []Key) {
	for i := 0; i < len(key); i++ {
		timestampKey, err := ToTimestampKey(key[i])
		if err != nil {
			normalKeys = append(normalKeys, TimestampKey(key[i]))
		} else {
			timestampKeys = append(timestampKeys, timestampKey)
		}
	}
	return
}

func CurrentTimestampNano() int64 {
	return time.Now().UnixNano()
}

type TestLogger struct {
	T *testing.T
}

func (t TestLogger) Debugf(format string, args ...interface{}) {
	t.T.Logf(format, args...)
}

func (t TestLogger) Errorf(format string, args ...interface{}) {
	t.T.Errorf(format, args...)
}

func (t TestLogger) Infof(format string, args ...interface{}) {
	t.T.Logf(format, args...)
}

func bytes2nano(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
