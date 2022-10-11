/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package main

import (
	"fmt"
	"os"
	"time"

	bn "chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/common/v2/report"
	"chainmaker.org/chainmaker/common/v2/shardingbirdsnest"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/go-echarts/go-echarts/v2/opts"
)

const (
	blockCap    = 10000
	totalHeight = 10000
)

func main() {
	_ = os.RemoveAll("./data")
	log := TestLog{}
	conf := &common.ShardingBirdsNestConfig{
		ChainId: "chain1",
		Length:  5,
		Timeout: 4,
		Birdsnest: &common.BirdsNestConfig{
			ChainId: "chain1",
			Length:  10,
			Rules:   &common.RulesConfig{AbsoluteExpireTime: 300},
			Cuckoo: &common.CuckooConfig{
				KeyType:       1,
				TagsPerBucket: 4,
				BitsPerItem:   9,
				MaxNumKeys:    2_000_000,
				TableType:     1,
			},
			Snapshot: &common.SnapshotSerializerConfig{
				Type:  common.SerializeIntervalType_Timed,
				Timed: &common.TimedSerializeIntervalConfig{Interval: 20},
				Path:  "./data/",
			},
		},
		Snapshot: &common.SnapshotSerializerConfig{
			Type:  common.SerializeIntervalType_Timed,
			Timed: &common.TimedSerializeIntervalConfig{Interval: 20},
			Path:  "./data/",
		},
	}
	sharding, err := shardingbirdsnest.NewShardingBirdsNest(conf, make(chan struct{}), bn.LruStrategy,
		shardingbirdsnest.NewModuloSA(5), log)
	if err != nil {
		log.Errorf("%v", log)
		return
	}
	heights := make([]uint64, 0, totalHeight)
	costs := make([]opts.BarData, 0, totalHeight)
	for i := uint64(0); i < totalHeight; i++ {
		keys := bn.GetTimestampKeys(blockCap)
		now := time.Now()
		err = sharding.AddsAndSetHeight(keys, i)
		if err != nil {
			log.Errorf("adds and set height, error: %v", err)
			return
		}
		cost := time.Since(now)
		costs = append(costs, opts.BarData{Value: cost.Nanoseconds()})
		heights = append(heights, i)
	}
	//_ = sharding.Serialize()
	//for _, nest := range sharding.bn {
	//	_ = nest.(bn.Serializer).Serialize()
	//}
	report.Report("Sharding bird's nest after optimization",
		"", heights, report.Series{Name: "Category A", Data: costs})

}

type TestLog struct {
}

func (t TestLog) Debugf(format string, args ...interface{}) {
	fmt.Println("[DEBUG] " + fmt.Sprintf(format, args...))
}

func (t TestLog) Errorf(format string, args ...interface{}) {
	fmt.Println("[ERROR] " + fmt.Sprintf(format, args...))
}

func (t TestLog) Infof(format string, args ...interface{}) {
	fmt.Println("[INFO] " + fmt.Sprintf(format, args...))
}
