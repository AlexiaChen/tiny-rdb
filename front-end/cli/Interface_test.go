package cli

import "testing"

func TestReadInput(t *testing.T) {
	var inputBuffer *InputBuffer
	inputBuffer = NewInputBuffer()
	ReadInput(inputBuffer)

	if inputBuffer.BufLen != 0 {
		t.Errorf("Input Buffer Len is not zero")
	}
}