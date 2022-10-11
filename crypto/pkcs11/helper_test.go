/*
Copyright (C) BABEC. All rights reserved.
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pkcs11

import (
	"testing"

	"github.com/miekg/pkcs11"
	"github.com/stretchr/testify/assert"
)

func TestFindObjects(t *testing.T) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_SECRET_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, internalAESKeyLabel),
	}
	objs, err := p11.findObjects(template, 10)
	assert.NoError(t, err)
	t.Logf("findObjects num = %d\n", len(objs))
}
