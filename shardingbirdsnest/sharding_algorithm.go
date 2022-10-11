/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package shardingbirdsnest

import (
	bn "chainmaker.org/chainmaker/common/v2/birdsnest"
)

// ChecksumKeyModulo uint32 checksum
func ChecksumKeyModulo(key bn.Key, length int) int {
	return int(key.Key()[key.Len()-1]) % length
}

type ModuloShardingAlgorithm struct {
	Length int
}

func NewModuloSA(l int) *ModuloShardingAlgorithm {
	return &ModuloShardingAlgorithm{Length: l}
}

// DoSharding 如果传入 shardingValues 小于 Length 则 最小设置为1
func (a ModuloShardingAlgorithm) DoSharding(shardingValues []bn.Key) [][]bn.Key {
	result := make([][]bn.Key, a.Length)
	// sharding
	for i := range shardingValues {
		modulo := a.DoShardingOnce(shardingValues[i])
		if result[modulo] == nil {
			result[modulo] = []bn.Key{shardingValues[i]}
		} else {
			result[modulo] = append(result[modulo], shardingValues[i])
		}
	}
	return result
}

func (a ModuloShardingAlgorithm) DoShardingOnce(key bn.Key) (index int) {
	return ChecksumKeyModulo(key, a.Length)
}
