/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand"
	"time"

	"chainmaker.org/chainmaker/common/v2/random/uuid"
	guuid "github.com/google/uuid"
)

const TestDir = "./data/timestamp_birds_nest"

func GetTimestampKey() Key {
	key, _ := ToTimestampKey(GenTimestampKey())
	return key
}
func GetTimestampKeyByNano(i int64) Key {
	key, _ := ToTimestampKey(GenTimestampKeyByNano(i))
	return key
}

func GenTimestampKey() string {
	return GenTimestampKeyByNano(time.Now().UnixNano())
}

func GenTimestampKeyByNano(nano int64) string {
	b := make([]byte, 16, 32)
	binary.BigEndian.PutUint64(b, uint64(nano))
	/*
		Read generates len(p) random bytes from the default Source and
		writes them into p. It always returns len(p) and a nil error.
		Read, unlike the Rand.Read method, is safe for concurrent use.
	*/
	b[8] = Separator
	// nolint: gosec
	_, _ = rand.Read(b[9:16])
	u := guuid.New()
	b = append(b, u[:]...)
	return hex.EncodeToString(b)
}

func GenTxId() string {
	return uuid.GetUUID() + uuid.GetUUID()
}

func GetTimestampKeys(n int) []Key {
	var keys []Key
	for i := 0; i < n; i++ {
		keys = append(keys, GetTimestampKey())
	}
	return keys
}
