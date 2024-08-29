//go:build use_s3 || !use_custom_driver
// +build use_s3 !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/s3"
