/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package shardingbirdsnest

import bn "chainmaker.org/chainmaker/common/v2/birdsnest"

type ShardingAlgorithm interface {
	DoSharding(shardingValues []bn.Key) [][]bn.Key
	DoShardingOnce(bn.Key) (index int)
}

type KeyModuloAlgorithm func(key bn.Key, length int) int
