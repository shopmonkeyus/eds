//go:build use_file || !use_custom_driver
// +build use_file !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/file"
