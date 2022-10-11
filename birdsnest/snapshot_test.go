/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"bytes"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadWrite(t *testing.T) {
	err := os.RemoveAll("./data/wal_snapshot")
	assert.Nil(t, err)
	snapshot, err := NewWalSnapshot("./data/wal_snapshot", Filepath, 0)
	assert.Nil(t, err)
	for i := 0; i < 10; i++ {
		write := []byte("aaa" + strconv.Itoa(i))
		err = snapshot.Write(write)
		assert.Nil(t, err)
		read, err := snapshot.Read()
		assert.Nil(t, err)
		if !bytes.Equal(write, read) {
			t.Errorf("got %v want %v", string(write), string(read))
		}
	}
}

func TestNewWalSnapshot(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "正常流",
			args:    args{"./data/wal_snapshot"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewWalSnapshot(tt.args.path, Filepath, 0)
			assert.Nil(t, err)
			assert.NotEmpty(t, got, "NewWalSnapshot() got is empty")
		})
	}
}
