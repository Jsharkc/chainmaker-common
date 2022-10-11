/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"time"
)

type Rule interface {
	Validate(Key) error
}

var (
	ErrKeyItsSoLongAgoError = NewError("key %v is out of the range %v-%v")
)

type AbsoluteExpireTimeRule struct {
	absoluteExpireTime int64
}

func (r AbsoluteExpireTimeRule) Validate(key Key) error {
	nano := key.GetNano()
	seconds := time.Now().UnixNano()
	start := seconds - r.absoluteExpireTime
	end := seconds + r.absoluteExpireTime
	if nano < start || nano > end {
		return ErrKeyItsSoLongAgoError.Error(key.String(), start, end)
	}
	return nil
}

func NewAETRule(absoluteExpireTime int64) AbsoluteExpireTimeRule {
	return AbsoluteExpireTimeRule{absoluteExpireTime: absoluteExpireTime * time.Second.Nanoseconds()}
}
