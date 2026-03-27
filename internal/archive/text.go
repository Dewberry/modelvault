package archive

import (
	"bytes"
	"unicode/utf8"
)

func isProbablyText(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return false
	}
	if !utf8.Valid(data) {
		return false
	}

	var controlCount int
	sample := data
	for len(sample) > 0 {
		r, size := utf8.DecodeRune(sample)
		sample = sample[size:]

		if r == utf8.RuneError && size == 1 {
			return false
		}
		if r == '\n' || r == '\r' || r == '\t' {
			continue
		}
		if r < 32 {
			controlCount++
		}
	}
	return controlCount < 5
}
