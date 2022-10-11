/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cert

import (
	"encoding/json"
	"sync"

	"github.com/pkg/errors"

	"chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/crypto/pkcs11"
)

var once sync.Once
var P11Context *pkcs11Context

type pkcs11Context struct {
	handle *pkcs11.P11Handle
	enable bool

	keyId   string
	keyType crypto.KeyType
}

func InitP11Handle(handle *pkcs11.P11Handle) {
	once.Do(func() {
		if P11Context == nil {
			P11Context = &pkcs11Context{
				handle: handle,
				enable: true,
			}
		}
	})
}

func (p *pkcs11Context) WithPrivKeyId(keyId string) *pkcs11Context {
	p.keyId = keyId
	return p
}

func (p *pkcs11Context) WithPrivKeyType(keyType crypto.KeyType) *pkcs11Context {
	p.keyType = keyType
	return p
}

type pkcs11KeySpec struct {
	KeyId   string `json:"key_id"`
	KeyType string `json:"key_type"`
}

// CreatePrivKey - create pkcs11 private key
func CreateP11Key(handle *pkcs11.P11Handle, keyType crypto.KeyType, keyId string) ([]byte, crypto.PrivateKey, error) {
	privKey, err := pkcs11.NewPrivateKey(handle, keyId, keyType)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to construct pkcs11 private key")
	}

	keySpec := &pkcs11KeySpec{
		KeyType: crypto.KeyType2NameMap[keyType],
		KeyId:   keyId,
	}
	keySpecJson, err := json.Marshal(keySpec)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "failed to get key spec json")
	}

	return keySpecJson, privKey, nil
}

func ParseP11PrivKey(handle *pkcs11.P11Handle, keySpecJson []byte) (crypto.PrivateKey, error) {
	var keySpec pkcs11KeySpec
	if err := json.Unmarshal(keySpecJson, &keySpec); err != nil {
		return nil, errors.WithMessage(err, "failed to parse pkcs11 keySpec")
	}

	return pkcs11.NewPrivateKey(handle, keySpec.KeyId, crypto.Name2KeyTypeMap[keySpec.KeyType])
}
