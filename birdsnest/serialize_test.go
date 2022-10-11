/*
Copyright (C) BABEC. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/
package birdsnest

import (
	"fmt"
	"os"
	"testing"
)

// TODO 偶尔报错
func TestBirdsNestImpl_Deserialize(t *testing.T) {
	err := os.RemoveAll("./data")
	if err != nil {
		fmt.Println(err)
	}
	type fields struct {
		bn *BirdsNestImpl
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr error
	}{
		{
			name: "异常流 修改配置",
			fields: fields{bn: func() *BirdsNestImpl {
				tbn := getTBN(TestDir+"_Deserialize", t)
				return tbn
			}()},
			wantErr: ErrCannotModifyTheNestConfiguration,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fields.bn.Serialize()
			if err != nil {
				t.Errorf("Serialize() error = %v", err)
			}
			tt.fields.bn.config.Snapshot.Timed.Interval = 20
			if err := tt.fields.bn.Deserialize(); err != tt.wantErr {
				t.Errorf("ExtensionDeserialize() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				t.Logf("ExtensionDeserialize() error = %v", err)
			}
		})
	}
}

func TestBirdsNestImpl_Start(t *testing.T) {
}

func TestBirdsNestImpl_serializeExit(t *testing.T) {
}

func TestBirdsNestImpl_serializeHeight(t *testing.T) {
}

func TestBirdsNestImpl_serializeMonitor(t *testing.T) {
}

func TestBirdsNestImpl_serializeTimed(t *testing.T) {
}

func TestBirdsNestImpl_timedAndExitSerialize(t *testing.T) {
}
