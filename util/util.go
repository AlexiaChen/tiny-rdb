package util

import "bytes"

// const var
const (
	ExitSuccess = 0
	ExitFailure = -1
)

// ToString [] byte to string
func ToString(byteStr []byte) string {
	n := bytes.IndexByte(byteStr, 0)
	return string(byteStr[:n])
}
