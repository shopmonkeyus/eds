package tracker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
)

func TestTracker(t *testing.T) {
	os.Remove(TrackerFilenameFromDir(os.TempDir()))
	logger := logger.NewTestLogger()
	tracker, err := NewTracker(TrackerConfig{
		Logger:  logger,
		Context: context.Background(),
		Dir:     os.TempDir(),
	})
	assert.NoError(t, err)
	assert.NotNil(t, tracker)
	ok, val, err := tracker.GetKey("foo")
	assert.False(t, ok)
	assert.NoError(t, err)
	assert.Empty(t, val)
	assert.NoError(t, tracker.SetKey("foo", "bar", time.Microsecond))
	time.Sleep(time.Millisecond * 2)
	ok, val, err = tracker.GetKey("foo")
	assert.NoError(t, err)
	assert.Empty(t, val)
	assert.False(t, ok)
	assert.NoError(t, tracker.SetKey("foo", "bar", 0))
	ok, val, err = tracker.GetKey("foo")
	assert.NoError(t, err)
	assert.NotEmpty(t, val)
	assert.True(t, ok)
	assert.Equal(t, "bar", val)
	assert.NoError(t, tracker.Close())
}
