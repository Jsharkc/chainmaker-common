/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package shardingbirdsnest

import (
	"errors"
	"path/filepath"
	"time"

	"go.uber.org/atomic"

	bn "chainmaker.org/chainmaker/common/v2/birdsnest"
	"chainmaker.org/chainmaker/pb-go/v2/common"
)

const (
	Filepath = "sharding"
)

var (
	ErrAddsTimeout                      = errors.New("add multiple key timeout")
	ErrCannotModifyTheNestConfiguration = errors.New("when historical data exists, you cannot modify the nest " +
		"configuration")
)

type ShardingBirdsNest struct {
	bn []bn.BirdsNest

	config    *common.ShardingBirdsNestConfig
	height    uint64
	preHeight *atomic.Uint64
	algorithm ShardingAlgorithm

	log        bn.Logger
	serializeC chan serializeSignal
	// TODO exit -> context.Context
	exitC    chan struct{}
	snapshot *bn.WalSnapshot
}

func NewShardingBirdsNest(config *common.ShardingBirdsNestConfig, exitC chan struct{}, strategy bn.Strategy,
	alg ShardingAlgorithm, logger bn.Logger) (*ShardingBirdsNest, error) {
	// eg: data/org1/tx_filter/chain1/sharding
	join := filepath.Join(config.Snapshot.Path, config.ChainId)
	snapshot, err := bn.NewWalSnapshot(join, Filepath, -1)
	if err != nil {
		return nil, err
	}
	s := &ShardingBirdsNest{
		algorithm:  alg,
		exitC:      exitC,
		config:     config,
		snapshot:   snapshot,
		log:        logger,
		preHeight:  atomic.NewUint64(0),
		serializeC: make(chan serializeSignal),
	}
	err = s.Deserialize()
	if err != nil {
		if err != ErrCannotModifyTheNestConfiguration {
			return nil, err
		}
	}
	birdsNests := make([]bn.BirdsNest, config.Length)
	for i := 0; i < int(config.Length); i++ {
		var birdsNest bn.BirdsNest
		birdsNest, err = bn.NewBirdsNestByNumber(config.Birdsnest, exitC, strategy, logger, i+1)
		if err != nil {
			if err != bn.ErrCannotModifyTheNestConfiguration {
				return nil, err
			}
		}
		birdsNests[i] = birdsNest
	}
	s.bn = birdsNests
	return s, err
}

func (s *ShardingBirdsNest) GetHeight() uint64 {
	return s.height
}

func (s *ShardingBirdsNest) SetHeight(height uint64) {
	s.height = height
	s.serializeHeight(height)
	for _, nest := range s.bn {
		nest.SetHeight(height)
	}
}

func (s *ShardingBirdsNest) AddsAndSetHeight(keys []bn.Key, height uint64) (result error) {
	err := s.Adds(keys)
	if err != nil {
		return err
	}
	s.SetHeight(height)
	return nil
}

func (s *ShardingBirdsNest) Adds(keys []bn.Key) (err error) {
	var (
		// sharding algorithm
		sharding = s.algorithm.DoSharding(keys)
		// finish channel
		finishC = make(chan int)
		// running task
		runningTask int
		// Timeout
		timeout = time.After(time.Duration(s.config.Timeout) * time.Second)
	)
	for i := 0; i < len(sharding); i++ {
		if sharding[i] == nil {
			continue
		}
		runningTask++
		go func(i int, values []bn.Key) {
			defer func() { finishC <- i }()
			err = s.bn[i].Adds(values)
		}(i, sharding[i])
	}
	for {
		select {
		case <-timeout:
			return ErrAddsTimeout
		case <-finishC:
			if err != nil {
				return
			}
			runningTask--
			if runningTask <= 0 {
				return
			}
		}
	}
}

func (s *ShardingBirdsNest) Add(key bn.Key) error {
	if key == nil || key.Len() == 0 {
		return bn.ErrKeyCannotBeEmpty
	}
	index := s.algorithm.DoShardingOnce(key)
	err := s.bn[index].Add(key)
	if err != nil {
		return err
	}
	return nil
}

func (s *ShardingBirdsNest) Contains(key bn.Key, rules ...common.RuleType) (bool, error) {
	if key == nil || key.Len() == 0 {
		return false, bn.ErrKeyCannotBeEmpty
	}
	index := s.algorithm.DoShardingOnce(key)
	contains, err := s.bn[index].Contains(key, rules...)
	if err != nil {
		return false, err
	}
	return contains, nil
}

func (s *ShardingBirdsNest) ValidateRule(key bn.Key, rules ...common.RuleType) error {
	if key == nil || key.Len() == 0 {
		return bn.ErrKeyCannotBeEmpty
	}
	// TODO Although each Bird's Nest is independent, the rules are consistent, so there is no need for sharding and 0
	// is used by default; If Bird's Nest rules are inconsistent for each shard in the future, open the following code
	// index := s.algorithm.DoShardingOnce(key)
	// err := s.bn[index].ValidateRule(key, rules...)
	err := s.bn[0].ValidateRule(key, rules...)
	if err != nil {
		return err
	}
	return nil
}

func (s *ShardingBirdsNest) Info() []uint64 {
	return nil
}

// Infos
// index 0 sharding index
// index 0 height
// index 1 cuckoo size
// index 2 current index
// index 3 total cuckoo size
// index 4 total space occupied by cuckoo
func (s *ShardingBirdsNest) Infos() [][]uint64 {
	infos := make([][]uint64, s.config.Length)
	for i, birdsNest := range s.bn {
		infos[i] = birdsNest.Info()
	}
	return infos
}
