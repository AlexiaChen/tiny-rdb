package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// InputBuffer Input buffer of REPL
type InputBuffer struct {
	buffer string
	bufLen int
}

// NewInputBuffer Make new input buffer
func NewInputBuffer() *InputBuffer {
	return new(InputBuffer)
}

// PrintPrompt Print CLI Prompt
func PrintPrompt() {
	fmt.Printf("tiny-rdb> ")
}

// const var
const (
	ExitSuccess = 0
	ExitFailure = -1
)

// ReadInput Read input line from stdin
func ReadInput(buf *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	buf.buffer, _ = reader.ReadString('\n')

	buf.buffer = strings.TrimSpace(buf.buffer)

	bufferLen := len(buf.buffer)
	if bufferLen == 0 {
		os.Exit(ExitFailure)
	}

	buf.bufLen = bufferLen
}

func main() {
	inputBuffer := NewInputBuffer()

	for {
		PrintPrompt()
		ReadInput(inputBuffer)

		if inputBuffer.buffer == "exit" || inputBuffer.buffer == "quit" {
			os.Exit(ExitSuccess)
		} else {
			fmt.Printf("Unrecognized command：%s\n", inputBuffer.buffer)
		}
	}

	ReadInput(inputBuffer)
}
