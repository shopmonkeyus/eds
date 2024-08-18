package util

import (
	"os"
	"runtime/pprof"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/shopmonkeyus/go-common/logger"
)

// The call stack here is usually:
// - panicError
// - RecoverPanic
// - panic()
// so RecoverPanic should pop three frames.
var depth = 3

// RecoverPanic recovers from a panic and logs the error along with the current goroutines.
func RecoverPanic(logger logger.Logger) {
	if r := recover(); r != nil {
		v := panicError(depth, r)
		var str strings.Builder
		pprof.Lookup("goroutine").WriteTo(&str, 2)
		logger.Error("a panic has occurred: %s\ncurrent goroutines:\n\n%s", v, str.String())
		os.Exit(2) // same exit code as panic
	}
}

func panicError(depth int, r interface{}) error {
	if err, ok := r.(error); ok {
		return errors.WithStackDepth(err, depth+1)
	}
	return errors.NewWithDepthf(depth+1, "panic: %v", r)
}
