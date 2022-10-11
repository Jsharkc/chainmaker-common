/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sm2

import (
	crypto2 "crypto"
	"encoding/pem"

	"chainmaker.org/chainmaker/common/v2/opencrypto/utils"

	bccrypto "chainmaker.org/chainmaker/common/v2/crypto"
	"chainmaker.org/chainmaker/common/v2/crypto/hash"
	"chainmaker.org/chainmaker/common/v2/opencrypto/gmssl/gmssl"
	"chainmaker.org/chainmaker/common/v2/opencrypto/gmssl/gmssl/sm3"
)

var _ bccrypto.PrivateKey = (*PrivateKey)(nil)

type PrivateKey struct {
	*gmssl.PrivateKey

	Pub PublicKey
}

func (sk *PrivateKey) Bytes() ([]byte, error) {
	skPem, err := sk.GetUnencryptedPEM()
	if err != nil {
		return nil, err
	}
	p, _ := pem.Decode([]byte(skPem))
	return p.Bytes, nil
}

func (sk *PrivateKey) Type() bccrypto.KeyType {
	return bccrypto.SM2
}

func (sk *PrivateKey) String() (string, error) {
	return sk.GetUnencryptedPEM()
}

func (sk *PrivateKey) PublicKey() bccrypto.PublicKey {
	return &sk.Pub
}

func (sk *PrivateKey) Sign(msg []byte) ([]byte, error) {
	return sk.signWithSM3(msg, utils.SM2_DEFAULT_USER_ID)
}

func (sk *PrivateKey) SignWithOpts(msg []byte, opts *bccrypto.SignOpts) ([]byte, error) {
	if opts == nil {
		return sk.Sign(msg)
	}
	if opts.Hash == bccrypto.HASH_TYPE_SM3 && sk.Type() == bccrypto.SM2 {
		uid := opts.UID
		if len(uid) == 0 {
			uid = bccrypto.CRYPTO_DEFAULT_UID
		}
		return sk.signWithSM3(msg, uid)
	}
	dgst, err := hash.Get(opts.Hash, msg)
	if err != nil {
		return nil, err
	}
	return sk.Sign(dgst)

}

func (sk *PrivateKey) ToStandardKey() crypto2.PrivateKey {
	return &signer{PrivateKey: *sk}
}

// PrivateKey implements bccrypto.PrivateKey
func (sk *PrivateKey) signWithSM3(msg []byte, uid string) ([]byte, error) {
	sm2zid, err := sk.ComputeSM2IDDigest(uid)
	if err != nil {
		return nil, err
	}

	sm3Hash := sm3.New()
	sm3Hash.Write(sm2zid)
	sm3Hash.Write(msg)
	dgst := sm3Hash.Sum(nil)
	return sk.PrivateKey.Sign("sm2sign", dgst, nil)
}

var _ bccrypto.DecryptKey = (*PrivateKey)(nil)

func (sk *PrivateKey) Decrypt(ciphertext []byte) ([]byte, error) {
	return sk.PrivateKey.Decrypt("sm2encrypt-with-sm3", ciphertext, nil)
}

func (sk *PrivateKey) DecryptWithOpts(ciphertext []byte, opts *bccrypto.EncOpts) ([]byte, error) {
	return sk.Decrypt(ciphertext)
}

func (sk *PrivateKey) EncryptKey() bccrypto.EncryptKey {
	return &PublicKey{sk.Pub.PublicKey}
}
