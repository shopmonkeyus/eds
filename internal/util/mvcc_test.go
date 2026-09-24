package util

import (
	"strings"
	"testing"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeMVCCPadsToFixedWidth(t *testing.T) {
	// walltime.logical from the changefeed
	v := NormalizeMVCC("1720732611708587506.0000000000")
	assert.Equal(t, mvccWalltimeWidth+mvccLogicalWidth, len(v))
	assert.Equal(t, "01720732611708587506"+"0000000000", v)

	// pure integer nanos from the importer (no logical component)
	v = NormalizeMVCC("1720732611708587506")
	assert.Equal(t, mvccWalltimeWidth+mvccLogicalWidth, len(v))
	assert.Equal(t, "01720732611708587506"+"0000000000", v)

	assert.Equal(t, "", NormalizeMVCC(""))
}

func TestNormalizeMVCCLexicalOrderMatchesNumericOrder(t *testing.T) {
	// same walltime, higher logical must sort later
	a := NormalizeMVCC("100.1")
	b := NormalizeMVCC("100.2")
	assert.True(t, a < b)

	// a larger walltime sorts later even though it has fewer raw digits than a
	// shorter, higher-logical value would suggest
	c := NormalizeMVCC("99.9999999999")
	d := NormalizeMVCC("100.0")
	assert.True(t, c < d)

	// ties compare equal
	assert.Equal(t, NormalizeMVCC("100.0"), NormalizeMVCC("100.0"))
}

func TestEventVersionFallsBackToTimestamp(t *testing.T) {
	withMVCC := EventVersion(&internal.DBChangeEvent{MVCCTimestamp: "100.5", Timestamp: 1})
	assert.Equal(t, NormalizeMVCC("100.5"), withMVCC)

	// no mvcc: fall back to the event timestamp (ms) scaled to nanos
	fallback := EventVersion(&internal.DBChangeEvent{Timestamp: 1720732611708})
	assert.Equal(t, mvccWalltimeWidth+mvccLogicalWidth, len(fallback))
	assert.Equal(t, padLeft("1720732611708000000", mvccWalltimeWidth)+strings.Repeat("0", mvccLogicalWidth), fallback)

	assert.Equal(t, "", EventVersion(&internal.DBChangeEvent{}))
	assert.Equal(t, "", EventVersion(nil))
}
