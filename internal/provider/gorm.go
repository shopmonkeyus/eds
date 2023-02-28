package provider

import (
	"fmt"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type GormProvider struct {
	logger logger.Logger
	db     *gorm.DB
	opts   *ProviderOpts
}

var _ internal.Provider = (*GormProvider)(nil)

func NewGormProvider(logger logger.Logger, url string, opts *ProviderOpts) (internal.Provider, error) {
	driver, err := parseURLForProvider(url)
	if err != nil {
		return nil, err
	}
	// TODO: implement the other databases
	var dialector gorm.Dialector
	switch driver {
	case "postgresql":
		dialector = postgres.Open(url)
	case "sqlserver":
		dialector = sqlserver.Open(url)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", driver)
	}
	glogger := internal.NewGormLogAdapter(logger)
	db, err := gorm.Open(dialector, &gorm.Config{Logger: glogger})
	if err != nil {
		return nil, err
	}
	return &GormProvider{
		logger,
		db,
		opts,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *GormProvider) Start() error {
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *GormProvider) Stop() error {
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *GormProvider) Process(data datatypes.ChangeEventPayload) error {
	if p.opts != nil && p.opts.DryRun {
		p.logger.Info("[dry-run] would write: %v", data)
		return nil
	}
	started := time.Now()
	switch data.GetOperation() {
	case datatypes.ChangeEventInsert:
		{
			if err := p.db.Create(data.GetAfter()).Error; err != nil {
				return err
			} else {
				p.logger.Trace("inserted db record for msgid: %s, took %v", data.GetID(), time.Since(started))
			}
		}
	case datatypes.ChangeEventUpdate:
		{
			if err := p.db.Clauses(clause.OnConflict{
				UpdateAll: true,
			}).Create(data.GetAfter()).Error; err != nil {
				return err
			} else {
				p.logger.Trace("upserted db record for msgid: %s, took %v", data.GetID(), time.Since(started))
			}
		}
	case datatypes.ChangeEventDelete:
		{
			if err := p.db.Delete(data.GetBefore()).Error; err != nil {
				return err
			} else {
				p.logger.Trace("deleted db record for msgid: %s, took %v", data.GetID(), time.Since(started))
			}
		}
	}
	return nil
}

// Migrate will tell the provider to do any migration work and return an error or nil if ok
func (p *GormProvider) Migrate() error {
	for index, name := range v3.ModelNames {
		p.logger.Info("migrating: %s", name)
		if err := p.db.AutoMigrate(v3.ModelInstances[index]); err != nil {
			return fmt.Errorf("error: migration of model: %s failed with: %s", name, err)
		}
	}
	return nil
}
