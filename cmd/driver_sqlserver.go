//go:build use_sqlserver || !use_custom_driver
// +build use_sqlserver !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/sqlserver"
