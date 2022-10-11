/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"errors"
	"path/filepath"

	"go.uber.org/atomic"

	"chainmaker.org/chainmaker/pb-go/v2/common"
)

const (
	Filepath = "birdsnest"
)

var (
	ErrKeyCannotBeEmpty                 = errors.New("key cannot be empty")
	ErrCannotModifyTheNestConfiguration = errors.New("when historical data exists, you cannot modify the nest " +
		"configuration")
	ErrBirdsNestSizeCannotBeZero = errors.New("the size cannot be 0")
)

// BirdsNestImpl impl
type BirdsNestImpl struct {
	height    uint64
	preHeight *atomic.Uint64
	// A set of cuckoo filters, stored on a linked list
	filters []CuckooFilter
	// BirdsNest Bird's Nest configuration
	config *common.BirdsNestConfig
	// Max strategy function
	strategy Strategy
	// The cuckoo filter is currently operational for use by the Add method
	currentIndex int
	// rules Bird's Nest rule
	rules map[common.RuleType]Rule
	// log Logger wrapper
	log Logger

	exitC chan struct{}
	// serializeC serialize channel
	serializeC chan serializeSignal
	// snapshot Wal implementation
	snapshot *WalSnapshot
}

// NewBirdsNest Create a BirdsNest
func NewBirdsNest(config *common.BirdsNestConfig, exitC chan struct{}, strategy Strategy, logger Logger) (
	*BirdsNestImpl, error) {
	return NewBirdsNestByNumber(config, exitC, strategy, logger, -1)
}

// NewBirdsNestByNumber Create a numbered BirdsNest
func NewBirdsNestByNumber(config *common.BirdsNestConfig, exitC chan struct{}, strategy Strategy, logger Logger,
	number int) (*BirdsNestImpl, error) {
	if config.GetLength() <= 0 {
		return nil, ErrBirdsNestSizeCannotBeZero
	}
	// eg: data/org1/tx_filter/chain1/birdsnest1
	join := filepath.Join(config.Snapshot.Path, config.ChainId)
	snapshot, err := NewWalSnapshot(join, Filepath, number)
	if err != nil {
		return nil, err
	}
	bn := &BirdsNestImpl{
		height:       0,
		preHeight:    atomic.NewUint64(0),
		filters:      newCuckooFilters(config.Cuckoo, config.GetLength()+1),
		config:       config,
		currentIndex: 0,
		exitC:        exitC,
		serializeC:   make(chan serializeSignal),
		log:          logger,
		// There is currently only one rule
		rules: map[common.RuleType]Rule{
			common.RuleType_AbsoluteExpireTime: NewAETRule(config.Rules.AbsoluteExpireTime),
		},
		snapshot: snapshot,
		strategy: strategy,
	}
	err = bn.Deserialize()
	if err != nil {
		if err != ErrCannotModifyTheNestConfiguration {
			return nil, err
		}
	}
	return bn, err
}

// GetHeight get current height
func (b *BirdsNestImpl) GetHeight() uint64 {
	return b.height
}

// SetHeight set height
func (b *BirdsNestImpl) SetHeight(height uint64) {
	b.height = height
	b.serializeHeight(height)
}

// AddsAndSetHeight Add multiple Key and set height
func (b *BirdsNestImpl) AddsAndSetHeight(keys []Key, height uint64) (result error) {
	err := b.Adds(keys)
	if err != nil {
		return err
	}
	b.SetHeight(height)
	return nil
}

// Adds Add multiple Key
func (b *BirdsNestImpl) Adds(keys []Key) error {
	for _, k := range keys {
		err := b.Add(k)
		if err != nil {
			return err
		}
	}
	return nil
}

// Add a Key
func (b *BirdsNestImpl) Add(key Key) error {
	if key == nil || key.Len() == 0 {
		return ErrKeyCannotBeEmpty
	}
	for {
		var add bool
		// fullStrategy Execute strategy when cuckoo filter is full
		err := b.fullStrategy()
		if err != nil {
			return err
		}
		add, err = b.filters[b.currentIndex].Add(key)
		if err != nil {
			return err
		}
		if add {
			return nil
		}
	}
}

func (b *BirdsNestImpl) ValidateRule(key Key, rules ...common.RuleType) error {
	if key == nil || key.Len() == 0 {
		return ErrKeyCannotBeEmpty
	}
	for _, rule := range rules {
		r, ok := b.rules[rule]
		if !ok {
			continue
		}
		err := r.Validate(key)
		if err != nil {
			return err
		}
	}
	return nil
}

// fullStrategy Execute strategy when cuckoo filter is full
func (b *BirdsNestImpl) fullStrategy() error {
	if !b.filters[b.currentIndex].IsFull() {
		return nil
	}
	err := b.strategy(b)
	if err != nil {
		return err
	}
	return nil
}

// Info
// index 0 height
// index 1 cuckoo size
// index 2 current index
// index 3 total cuckoo size
// index 4 total space occupied by cuckoo
func (b *BirdsNestImpl) Info() []uint64 {
	var infos = make([]uint64, 5)
	infos[0] = b.height
	infos[1] = uint64(b.config.GetLength()) // cuckoo size
	infos[2] = uint64(b.currentIndex)       // current index
	for _, filter := range b.filters {
		info := filter.Info()
		infos[3] += info[0] // total keys size
		infos[4] += info[1] // total space
	}
	return infos
}

// Convert common.KeyType to common.FilterExtensionType
func statusConvertExtension(kt common.KeyType) common.FilterExtensionType {
	switch kt {
	case common.KeyType_KTDefault:
		return common.FilterExtensionType_FETDefault
	case common.KeyType_KTTimestampKey:
		return common.FilterExtensionType_FETTimestamp
	default:
		return -1
	}
}

// TODO 下一版优化 go Encode
func analysisCuckooFilters(f []CuckooFilter) ([]*common.CuckooFilter, error) {
	var filters = make([]*common.CuckooFilter, len(f))
	for i, filter := range f {
		encode, err := filter.Encode()
		if err != nil {
			return nil, err
		}
		filters[i] = &common.CuckooFilter{
			Cuckoo:    encode.filter,
			Extension: filter.Extension().Serialize(),
			Config:    encode.config,
		}
	}
	return filters, nil
}
