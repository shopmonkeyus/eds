//go:build use_snowflak || !use_custom_driver
// +build use_snowflak !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/snowflake"
