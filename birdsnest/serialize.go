/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"time"

	"chainmaker.org/chainmaker/pb-go/v2/common"
	"github.com/gogo/protobuf/proto"
)

// TODO Split BirdsNestImpl and Serialize

// Start TODO Goroutinue should be turned off using context.Context here
func (b *BirdsNestImpl) Start() {
	go b.serializeMonitor()
	go b.serializeTimed()
}

// serializeMonitor
func (b *BirdsNestImpl) serializeMonitor() {
	for { // nolint
		select {
		// Only signals for the current filter "serialized type" are received
		case signal := <-b.serializeC:
			t, ok := common.SerializeIntervalType_name[int32(signal.typ)]
			if !ok {
				b.log.Errorf("serialize type %v not support", t)
			}
			switch signal.typ {
			case common.SerializeIntervalType_Height:
				// current height - pre height < height interval does not serialize; otherwise, it serialize
				// eg: 85 - 80 = 5 < 10
				// 	   5 < 10 true does not serialize
				if b.height-b.preHeight.Load() < b.config.Snapshot.BlockHeight.Interval {
					continue
				}
			case common.SerializeIntervalType_Timed, common.SerializeIntervalType_Exit:
				// common.SerializeIntervalType_Timed and common.SerializeIntervalType_Exit are handled directly
			default:
				continue
			}
			err := b.Serialize()
			if err != nil {
				b.log.Errorf("serialize error type: %v, error: %v", t, err)
			}
		}
	}
}

// Serialize all cuckoos in the current BirdsNest
func (b *BirdsNestImpl) Serialize() error {
	t := time.Now()
	defer func(log Logger) {
		elapsed := time.Since(t)
		log.Debugf("bird's nest serialize success elapsed: %v", elapsed)
	}(b.log)
	// convert []CuckooFilter to []*common.CuckooFilter
	var filters []*common.CuckooFilter
	filters, err := analysisCuckooFilters(b.filters)
	if err != nil {
		return err
	}
	birdsNest := &common.BirdsNest{
		Config:       b.config,
		Height:       b.preHeight.Load(),
		CurrentIndex: uint32(b.currentIndex),
		Filters:      filters,
	}
	data, err := proto.Marshal(birdsNest)
	if err != nil {
		return err
	}
	err = b.snapshot.Write(data)
	if err != nil {
		return err
	}
	b.preHeight.Store(b.height)
	return nil
}

func (b *BirdsNestImpl) Deserialize() error {
	data, err := b.snapshot.Read()
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	var bn = new(common.BirdsNest)
	err = proto.Unmarshal(data, bn)
	if err != nil {
		return err
	}
	filters, err := newCuckooFiltersByDecode(bn.Filters)
	if err != nil {
		return err
	}
	if !proto.Equal(bn.Config, b.config) {
		err = ErrCannotModifyTheNestConfiguration
	}
	b.filters = filters
	b.config = bn.Config
	b.height = bn.Height
	b.currentIndex = int(bn.CurrentIndex)
	return err
}

func (b *BirdsNestImpl) serializeTimed() {
	if b.config.Snapshot.Type != common.SerializeIntervalType_Timed {
		return
	}
	ticker := time.NewTicker(time.Second * time.Duration(b.config.Snapshot.Timed.Interval))
	// nolint
	for {
		select {
		case <-ticker.C:
			b.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Timed}
		}
	}
}

// nolint
func (b *BirdsNestImpl) serializeExit() {
	b.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Exit}
}

func (b *BirdsNestImpl) serializeHeight(height uint64) {
	if b.config.Snapshot.Type != common.SerializeIntervalType_Height {
		return
	}
	b.serializeC <- serializeSignal{typ: common.SerializeIntervalType_Height, height: height}
}

// Serialize signal
type serializeSignal struct {
	typ    common.SerializeIntervalType
	height uint64
}
