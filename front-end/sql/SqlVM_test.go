package sql

import (
	"testing"
	"tiny-rdb/front-end/cli"
)

func TestRunRawCommand(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "testCmd"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	if RunRawCommand(inputBuffer) != RawCommandUnrecognizedCMD {
		t.Errorf("Command is not unrecognized command")
	}

	inputBuffer.Buffer = "#other"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	if RunRawCommand(inputBuffer) != RawCommandSuccess {
		t.Errorf("Command is not success command")
	}

}

func TestPrepareStatement(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "insert"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	var statement Statement
	result := PrepareStatement(inputBuffer, &statement)

	if result != PrepareSuccess {
		t.Errorf("result must be success")
	}

	if statement.Type != InsertStatement {
		t.Errorf("statement type must be insert statement")
	}

	inputBuffer.Buffer = "select"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	result = PrepareStatement(inputBuffer, &statement)

	if result != PrepareSuccess {
		t.Errorf("result must be success")
	}

	if statement.Type != SelectStatement {
		t.Errorf("statement type must be insert statement")
	}

	inputBuffer.Buffer = "unkown"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	result = PrepareStatement(inputBuffer, &statement)

	if result != PrepareUnrecognizedStatement {
		t.Errorf("result must be unrecognized statement")
	}

}
