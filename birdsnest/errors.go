/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"fmt"
)

type SprintfError string

func (s SprintfError) Error(a ...interface{}) error {
	return fmt.Errorf(string(s), a...)
}

func NewError(s string) SprintfError {
	return SprintfError(s)
}
