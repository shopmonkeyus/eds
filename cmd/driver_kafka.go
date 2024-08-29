//go:build use_kafka || !use_custom_driver
// +build use_kafka !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/kafka"
