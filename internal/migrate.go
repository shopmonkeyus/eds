package internal

import (
	"context"
	"fmt"

	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Migrate will run migration for all tables
func Migrate(logger logger.Interface, db *gorm.DB) error {
	for index, name := range v3.ModelNames {
		logger.Info(context.Background(), "migrating: %s", name)
		if err := db.AutoMigrate(v3.ModelInstances[index]); err != nil {
			return fmt.Errorf("error: migration of model: %s failed with: %s", name, err)
		}
	}
	return nil
}
