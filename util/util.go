package util

import (
	"bytes"
	"math/rand"
)

// const var
const (
	ExitSuccess = 0
	ExitFailure = -1
)

// ToString convert null-terminated byte array to string
func ToString(byteStr []byte) string {
	n := bytes.IndexByte(byteStr, 0)
	return string(byteStr[:n])
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandString generate fixed-length random string
func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
