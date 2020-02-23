package sql

import (
	"testing"
	"tiny-rdb/back-end/table"
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
	inputBuffer.Buffer = "insert 12 chen we@qq.com"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	var statement Statement
	result := PrepareStatement(inputBuffer, &statement)

	if result != PrepareSuccess {
		t.Errorf("result must be success: %v", result)
	}

	if statement.Type != InsertStatement {
		t.Errorf("statement type must be insert statement")
	}

	if statement.RowToInsert.PrimaryID != 12 {
		t.Errorf("statement row insert primary id must be 12")
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

func TestInsertAndSelect(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "insert 12 chen we@qq.com"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	table := new(table.Table)
	var statement Statement
	result := PrepareStatement(inputBuffer, &statement)

	if result != PrepareSuccess {
		t.Errorf("result must be success: %v", result)
	}

	result = RunStatement(table, &statement)
	if result != ExecuteSuccess {
		t.Errorf("result must be execute success: %v", result)
	}

	inputBuffer.Buffer = "select"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	var selectState Statement
	result = PrepareStatement(inputBuffer, &selectState)
	if result != PrepareSuccess {
		t.Errorf("result must be success: %v", result)
	}

	result = RunStatement(table, &selectState)
	if result != ExecuteSuccess {
		t.Errorf("result must be execute success: %v", result)
	}

}
