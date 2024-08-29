//go:build use_eventhub || !use_custom_driver
// +build use_eventhub !use_custom_driver

package cmd

import _ "github.com/shopmonkeyus/eds/internal/drivers/eventhub"
