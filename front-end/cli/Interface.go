package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// InputBuffer Input buffer of REPL
type InputBuffer struct {
	Buffer string
	BufLen int
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
	buf.Buffer, _ = reader.ReadString('\n')

	buf.Buffer = strings.TrimSpace(buf.Buffer)
	buf.BufLen = len(buf.Buffer)
}
