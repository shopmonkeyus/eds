package util

import (
	"fmt"
	"hash/fnv"

	"github.com/cespare/xxhash/v2"
	gstr "github.com/savsgio/gotils/strconv"
)

// Hash will take one or more values and return a xxhash calculated value for the input
func Hash(vals ...interface{}) string {
	h := xxhash.New()
	for _, v := range vals {
		h.Write(gstr.S2B(fmt.Sprintf("%+v", v)))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Modulo will take the value and return a modulo with the num length
func Modulo(value string, num int) int {
	hasher := fnv.New32a()
	hasher.Write(gstr.S2B(value))
	partition := int(hasher.Sum32()) % num
	if partition < 0 {
		partition = -partition
	}
	return partition
}
