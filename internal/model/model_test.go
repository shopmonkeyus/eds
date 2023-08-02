package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexName(t *testing.T) {
	name := GenerateIndexName("location_payment_fee", []string{"locationPaymentConfigId", "cardType", "paymentMode"}, "key")
	assert.Equal(t, "location_payment_fee_locationPaymentConfigId_cardType_payme_key", name)
}

func TestIndexNamePrimaryKey(t *testing.T) {
	name := GenerateIndexName("location_payment_fee", []string{}, "pkey")
	assert.Equal(t, "location_payment_fee_pkey", name)
}
