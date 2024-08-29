//go:build use_postgres || use_postgresql || !use_custom_driver
// +build use_postgres use_postgresql !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/postgresql"
