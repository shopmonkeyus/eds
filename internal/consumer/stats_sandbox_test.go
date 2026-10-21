package consumer

import (
	"testing"

	"github.com/shopmonkeyus/eds/internal"
)

func TestSystemStatsSandbox(t *testing.T) {
	s, err := internal.GetSystemStats()
	t.Logf("stats=%+v err=%v", s, err)
}
