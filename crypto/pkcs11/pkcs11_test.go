/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"fmt"
	"math/big"
	"os"
	"runtime"
	"testing"

	bccrypto "chainmaker.org/chainmaker/common/v2/crypto"

	"github.com/stretchr/testify/assert"
)

type signature struct {
	R, S *big.Int
}

var (
	support_GM          = false
	plain               = []byte("chainmaker")
	internalSM2KeyLabel = []byte("SM2SignKey1")
	internalRSAKeyLabel = []byte("RSASignKey1")
	internalAESKeyLabel = []byte("MasterKey1")
	internalSM4KeyLabel = []byte("MasterKey2")
)

var (
	lib              = "/usr/lib64/libsofthsm2.so"
	label            = "test"
	password         = "1234"
	sessionCacheSize = 10
	hashStr          = "SHA1"
)

var (
	p11 *P11Handle
)

func TestMain(m *testing.M) {
	//set lib path
	if runtime.GOOS == "darwin" {
		lib = "/usr/local/Cellar/softhsm/2.6.1/lib/softhsm/libsofthsm2.so"
		if runtime.GOARCH == "arm64" {
			lib = "/opt/homebrew/Cellar/softhsm/2.6.1/lib/softhsm/libsofthsm2.so"
		}
	}

	var err error
	p11, err = New(lib, label, password, sessionCacheSize, hashStr)
	if err != nil || p11 == nil {
		fmt.Printf("Init pkcs11 handle fail, err = %s\n", err)
		os.Exit(1)
	}
	if err := genTestKeys(); err != nil {
		fmt.Printf("Init test keys fail, err = %s\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestFindSlotLabel(t *testing.T) {
	labels, err := listSlot(p11.ctx)
	assert.NoError(t, err)
	fmt.Printf("%v\n", labels)
}

func genTestKeys() error {
	if support_GM {
		if _, err := GenKeyPair(p11, string(internalSM2KeyLabel), bccrypto.SM2, nil); err != nil {
			return err
		}
		if _, err := GenSecretKey(p11, string(internalSM4KeyLabel), bccrypto.SM4, 16); err != nil {
			return err
		}
	}
	if _, err := GenKeyPair(p11, string(internalRSAKeyLabel), bccrypto.RSA1024, &GenOpts{KeyBits: 1024}); err != nil {
		return err
	}
	if _, err := GenSecretKey(p11, string(internalAESKeyLabel), bccrypto.AES, 16); err != nil {
		return err
	}
	return nil
}
