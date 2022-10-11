/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package shardingbirdsnest

import (
	"time"

	bn "chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/gogo/protobuf/proto"
)

// Start TODO Goroutinue should be turned off using context.Context here
func (s *ShardingBirdsNest) Start() {
	go s.serializeMonitor()
	go s.serializeTimed()
	// start all bird's nest
	for i := range s.bn {
		s.bn[i].Start()
	}
}

func (s *ShardingBirdsNest) Serialize() error {
	t := time.Now()
	defer func(log bn.Logger) {
		elapsed := time.Since(t)
		log.Infof("sharding bird's nest serialize success elapsed: %v", elapsed)
	}(s.log)

	sbn := &common.ShardingBirdsNest{
		Length: s.config.Length,
		Height: s.height,
		Config: s.config,
	}
	data, err := proto.Marshal(sbn)
	if err != nil {
		return err
	}
	err = s.snapshot.Write(data)
	if err != nil {
		return err
	}
	s.preHeight.Store(s.height)
	return nil
}

func (s *ShardingBirdsNest) Deserialize() error {
	data, err := s.snapshot.Read()
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	sharding := new(common.ShardingBirdsNest)
	err = proto.Unmarshal(data, sharding)
	if err != nil {
		return err
	}
	if proto.Equal(sharding.Config, s.config) {
		err = ErrCannotModifyTheNestConfiguration
	}
	s.height = sharding.Height
	return err
}

// serializeMonitor TODO Goroutinue should be turned off using context.Context here
func (s *ShardingBirdsNest) serializeMonitor() {
	for { // nolint
		select {
		// 只有当前"序列化类型"的信号才能过来
		case signal := <-s.serializeC:
			t, ok := common.SerializeIntervalType_name[int32(signal.typ)]
			if !ok {
				s.log.Errorf("serialize type %v not support", t)
			}
			switch signal.typ {
			case common.SerializeIntervalType_Height:
				// 并且 当前高度 - 上次持久化高度 < 高度间隔 则不做持久化 否则，执行持久化
				// eg: 85 - 80 = 5 < 10
				// 	   5 < 10 true 则不做持久化
				if s.height-s.preHeight.Load() < s.config.Snapshot.BlockHeight.Interval {
					continue
				}
			case common.SerializeIntervalType_Timed, common.SerializeIntervalType_Exit:
				// "时间序列化类型"和"退出序列化类型"直接处理
			default:
				continue
			}
			err := s.Serialize()
			if err != nil {
				s.log.Errorf("serialize error type: %v, error: %v", t, err)
			}
		}
	}
}

func (s *ShardingBirdsNest) serializeTimed() {
	if s.config.Snapshot.Type != common.SerializeIntervalType_Timed {
		return
	}
	ticker := time.NewTicker(time.Second * time.Duration(s.config.Snapshot.Timed.Interval))
	// nolint
	for {
		select {
		case <-ticker.C:
			s.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Timed}
		}
	}
}

// nolint
func (s *ShardingBirdsNest) serializeExit() {
	s.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Exit}
}

func (s *ShardingBirdsNest) serializeHeight(height uint64) {
	if s.config.Snapshot.Type != common.SerializeIntervalType_Height {
		return
	}
	s.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Height, height: height}
}

// Serialize signal
type serializeSignal struct {
	typ    common.SerializeIntervalType
	height uint64
}
