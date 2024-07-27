package tracker

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/tidwall/buntdb"
)

type TrackerConfig struct {
	Context context.Context
	Logger  logger.Logger
	Dir     string
}

type Tracker struct {
	ctx    context.Context
	logger logger.Logger
	db     *buntdb.DB
	once   sync.Once
}

// Close will close the tracker and the underlying database.
func (t *Tracker) Close() error {
	t.logger.Debug("closing")
	t.once.Do(func() {
		t.db.Shrink()
		t.db.Close()
	})
	t.logger.Debug("closed")
	return nil
}

// GetKey will return the value of the key from the database.
func (t *Tracker) GetKey(key string) (bool, string, error) {
	var value string
	var found bool
	err := t.db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get(key, false)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return nil
			}
			return err
		}
		value = val
		found = true
		return nil
	})
	if err != nil {
		return found, "", fmt.Errorf("failed to get key: %w", err)
	}
	return found, value, nil
}

// SetKey will set the key to the value in the database.
func (t *Tracker) SetKey(key, value string, expires time.Duration) error {
	err := t.db.Update(func(tx *buntdb.Tx) error {
		var opts *buntdb.SetOptions
		if expires > 0 {
			opts = &buntdb.SetOptions{Expires: true, TTL: expires}
		}
		_, _, err := tx.Set(key, value, opts)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to set key: %w", err)
	}
	return nil
}

// SetKeys will set multiple key to the same value in the database.
func (t *Tracker) SetKeys(keys []string, value string, expires time.Duration) error {
	err := t.db.Update(func(tx *buntdb.Tx) error {
		var opts *buntdb.SetOptions
		if expires > 0 {
			opts = &buntdb.SetOptions{Expires: true, TTL: expires}
		}
		for _, key := range keys {
			if _, _, err := tx.Set(key, value, opts); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to set keys: %w", err)
	}
	return nil
}

// DeleteKey will delete the key from the database.
func (t *Tracker) DeleteKey(keys ...string) error {
	return t.db.Update(func(tx *buntdb.Tx) error {
		for _, key := range keys {
			if _, err := tx.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// TrackerFilenameFromDir returns the filename for the tracker database based on a specific directory.
func TrackerFilenameFromDir(dir string) string {
	return filepath.Join(dir, "eds-server-data.db")
}

// NewTracker will create a new tracker with the given configuration.
func NewTracker(config TrackerConfig) (*Tracker, error) {
	var tracker Tracker

	db, err := buntdb.Open(TrackerFilenameFromDir(config.Dir))
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	var dbcfg buntdb.Config
	if err := db.ReadConfig(&dbcfg); err != nil {
		return nil, fmt.Errorf("failed to read db config: %w", err)
	}
	dbcfg.SyncPolicy = buntdb.EverySecond
	if err := db.SetConfig(dbcfg); err != nil {
		log.Fatal(err)
	}

	tracker.db = db
	tracker.ctx = config.Context
	tracker.logger = config.Logger.WithPrefix("[tracker]")

	return &tracker, nil
}
