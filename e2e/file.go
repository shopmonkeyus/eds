//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
)

type fileProviderRunner struct {
	logger logger.Logger
	dir    string
}

var _ TestProviderRunner = (*fileProviderRunner)(nil)

func NewFileTestProviderRunner(logger logger.Logger) TestProviderRunner {
	return &fileProviderRunner{logger, ""}
}

func (r *fileProviderRunner) Start() error {
	r.dir = path.Join(os.TempDir(), fmt.Sprintf("test-%v", time.Now()))
	os.MkdirAll(r.dir, 0755)
	r.logger.Info("writing files to: %s", r.dir)
	return nil
}

func (r *fileProviderRunner) Stop() error {
	os.RemoveAll(r.dir)
	return nil
}

func (r *fileProviderRunner) URL() string {
	return "file://" + r.dir
}

// Validate that the object was committed to storage
func (r *fileProviderRunner) Validate(object datatypes.ChangeEventPayload) error {
	fn := path.Join(r.dir, object.GetTable()+"_"+object.GetMvccTimestamp()+"_"+object.GetID()+".json.gz")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return fmt.Errorf("error finding file: %s. %w", fn, err)
	}
	buf1, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("error reading file: %s. %w", fn, err)
	}
	buf2, _ := json.Marshal(object.GetAfter())
	if err := compareJSON(buf1, buf2); err != nil {
		return err
	}
	return nil
}
