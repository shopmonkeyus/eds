//go:build use_mysql || !use_custom_driver
// +build use_mysql !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/mysql"
