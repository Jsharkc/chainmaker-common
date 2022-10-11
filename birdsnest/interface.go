/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import "chainmaker.org/chainmaker/pb-go/v2/common"

type Serializer interface {
	Serialize() error
	Deserialize() error
}

// BirdsNest Bird's Nest
type BirdsNest interface {
	GetHeight() uint64
	SetHeight(height uint64)
	// Add the key
	Add(key Key) error
	// Adds adding Multiple Keys
	Adds(keys []Key) (result error)
	// AddsAndSetHeight Adds and SetHeight
	AddsAndSetHeight(keys []Key, height uint64) (result error)
	// Contains the key
	Contains(key Key, rules ...common.RuleType) (bool, error)
	ValidateRule(key Key, rules ...common.RuleType) error
	// Info Current cuckoos nest information and status
	Info() []uint64

	Start()
}

type CuckooFilter interface {
	IsFull() bool
	Add(key Key) (bool, error)
	Contains(key Key) (bool, error)
	Encode() (FilterEncoder, error)
	Extension() FilterExtension
	Info() []uint64
}

// FilterExtension filter extension
type FilterExtension interface {
	// Validate validate key
	Validate(key Key, full bool) error
	Store(key Key) error
	Serialize() []byte
}

type Logger interface {
	Debugf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}
