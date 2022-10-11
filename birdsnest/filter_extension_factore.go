/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"sync"

	"chainmaker.org/chainmaker/pb-go/v2/common"
)

var (
	ErrFilterExtensionNotSupport = NewError("filter extension not support type: %v")
)

type factory struct {
}

var once sync.Once
var _instance *factory

// Factory return the global tx filter factory.
//nolint: revive
func Factory() *factory {
	once.Do(func() { _instance = new(factory) })
	return _instance
}

func (cf *factory) New(fet common.FilterExtensionType) (FilterExtension, error) {
	switch fet {
	case common.FilterExtensionType_FETDefault:
		return NewDefaultFilterExtension(), nil
	case common.FilterExtensionType_FETTimestamp:
		return NewTimestampFilterExtension(), nil
	default:
		return nil, ErrFilterExtensionNotSupport.Error(fet)
	}
}
