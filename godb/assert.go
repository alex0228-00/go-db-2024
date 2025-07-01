package godb

import (
	"fmt"
	"strings"
)

func assert(cond bool, msg ...interface{}) {
	if !cond {
		strs := make([]string, len(msg))
		for i, v := range msg {
			strs[i] = fmt.Sprint(v)
		}
		panic(fmt.Sprintf("assertion failed: %s", strings.Join(strs, ";")))
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
