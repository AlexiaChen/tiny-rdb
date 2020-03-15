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

// ReadInput Read input line from stdin
func ReadInput(buf *InputBuffer) {
	reader := bufio.NewReader(os.Stdin)
	buf.Buffer, _ = reader.ReadString('\n')

	buf.Buffer = strings.TrimSpace(buf.Buffer)
	buf.BufLen = len(buf.Buffer)
}

func IsRawCommand(cmd *string) bool {
	if (*cmd)[0] == '#' {
		return true
	} else {
		return false
	}
}
