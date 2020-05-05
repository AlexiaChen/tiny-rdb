package sql

import (
	"fmt"
	"os"
	"testing"
	"tiny-rdb/backend"
	"tiny-rdb/frontend/cli"
	"tiny-rdb/util"
)

func TestRunRawCommand(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "testCmd"
	inputBuffer.BufLen = len(inputBuffer.Buffer)
	dbFile := "./RawCmd.db"
	table := backend.OpenDB(dbFile)
	if RunRawCommand(inputBuffer, table) != RawCommandUnrecognizedCMD {
		t.Errorf("Command is not unrecognized command")
	}

	inputBuffer.Buffer = "#other"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	if RunRawCommand(inputBuffer, table) != RawCommandSuccess {
		t.Errorf("Command is not success command")
	}

	inputBuffer.Buffer = "#btree"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	if RunRawCommand(inputBuffer, table) != RawCommandSuccess {
		t.Errorf("Command is not success command")
	}

	backend.CloseDB(table)
	os.Remove(dbFile)

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

	inputBuffer.Buffer = "insert 15 xsssliuliuliuliuyifeifeifeifeifei kk@google.com"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	result = PrepareStatement(inputBuffer, &statement)
	if result != PrepareStringTooLong {
		t.Errorf("UserName must be too long: %v", len(util.ToString(statement.RowToInsert.UserName[:])))
	}

}

func TestInsertAndSelect(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "insert 12 chen we@qq.com"
	inputBuffer.BufLen = len(inputBuffer.Buffer)
	dbFile := "./InsertAndSelect.db"
	table := backend.OpenDB(dbFile)
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

	var cursor *backend.Cursor = backend.CursorBegin(table)
	for !cursor.IsEndOfTable {
		var row backend.Row
		var readableRow backend.VisualRow

		rowSlotSlice := backend.CursorValue(cursor)
		rowSize := backend.DeserializeRow(rowSlotSlice, &row)
		if rowSize != backend.RowSize {
			t.Errorf("Row Size Error: %v", rowSize)
		}

		readableRow.PrimaryID = row.PrimaryID
		readableRow.UserName = util.ToString(row.UserName[:])
		readableRow.Email = util.ToString(row.Email[:])

		if readableRow.PrimaryID != 12 || readableRow.UserName != "chen" || readableRow.Email != "we@qq.com" {
			t.Errorf("Row (%v, %s, %s) Error", readableRow.PrimaryID, readableRow.UserName, readableRow.Email)
		}

		backend.CursorNext(cursor)
	}

	backend.CloseDB(table)
	os.Remove(dbFile)

}

func TestBunchOfInsert(t *testing.T) {
	dbFile := "./BunchOfInsert.db"
	table := backend.OpenDB(dbFile)
	inputBuffer := cli.NewInputBuffer()
	InsertNum := uint32(100)
	for i := uint32(0); i < InsertNum; i++ {

		inputBuffer.Buffer = fmt.Sprintf("insert %d %s %s", i, util.RandString(8), util.RandString(8)+"@google.com")
		inputBuffer.BufLen = len(inputBuffer.Buffer)

		var statement Statement
		result := PrepareStatement(inputBuffer, &statement)

		if result != PrepareSuccess {
			t.Errorf("result must be success: %v", result)
		}

		result = RunStatement(table, &statement)
		if result != ExecuteSuccess {
			t.Errorf("result must be execute success: %v", result)
		}
	}

	var endCursor *backend.Cursor = backend.CursorEnd(table)
	if endCursor.PassedCells != InsertNum {
		t.Errorf("Cell Num must be %v, but it is %v", InsertNum, endCursor.PassedCells)
	}

	inputBuffer.Buffer = "select"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	var selectState Statement
	result := PrepareStatement(inputBuffer, &selectState)
	if result != PrepareSuccess {
		t.Errorf("result must be success: %v", result)
	}

	result = RunStatement(table, &selectState)
	if result != ExecuteSuccess {
		t.Errorf("result must be execute success: %v", result)
	}

	backend.CloseDB(table)

	tableNew := backend.OpenDB(dbFile)

	endCursor = backend.CursorEnd(tableNew)
	if endCursor.PassedCells != InsertNum {
		t.Errorf("Cell Num must be %v, but it is %v", InsertNum, endCursor.PassedCells)
	}

	backend.CloseDB(tableNew)
	os.Remove(dbFile)
}

func TestDuplicateKey(t *testing.T) {
	dbFile := "./DuplicateKey.db"
	table := backend.OpenDB(dbFile)
	inputBuffer := cli.NewInputBuffer()
	InsertNum := uint32(10)

	for i := uint32(0); i < InsertNum; i++ {

		inputBuffer.Buffer = fmt.Sprintf("insert %d %s %s", i, util.RandString(8), util.RandString(8)+"@google.com")
		inputBuffer.BufLen = len(inputBuffer.Buffer)

		var statement Statement
		result := PrepareStatement(inputBuffer, &statement)

		if result != PrepareSuccess {
			t.Errorf("result must be success: %v", result)
		}

		result = RunStatement(table, &statement)
		if result != ExecuteSuccess {
			t.Errorf("result must be execute success: %v", result)
		}
	}

	backend.CloseDB(table)

	tableNew := backend.OpenDB(dbFile)

	for i := uint32(0); i < InsertNum; i++ {

		inputBuffer.Buffer = fmt.Sprintf("insert %d %s %s", i, util.RandString(8), util.RandString(8)+"@google.com")
		inputBuffer.BufLen = len(inputBuffer.Buffer)

		var statement Statement
		result := PrepareStatement(inputBuffer, &statement)

		if result != PrepareSuccess {
			t.Errorf("result must be success: %v", result)
		}

		result = RunStatement(tableNew, &statement)
		if result != ExecuteDuplicateKey {
			t.Errorf("result must be execute Duplicate Key: %v", result)
		}
	}

	backend.CloseDB(tableNew)
	os.Remove(dbFile)

}

func TestOrderedKey(t *testing.T) {
	dbFile := "./OrderedKey.db"
	table := backend.OpenDB(dbFile)
	inputBuffer := cli.NewInputBuffer()
	InsertNum := uint32(10)

	for i := InsertNum; i > 0; i-- {

		inputBuffer.Buffer = fmt.Sprintf("insert %d %s %s", i, util.RandString(8), util.RandString(8)+"@google.com")
		inputBuffer.BufLen = len(inputBuffer.Buffer)

		var statement Statement
		result := PrepareStatement(inputBuffer, &statement)

		if result != PrepareSuccess {
			t.Errorf("result must be success: %v", result)
		}

		result = RunStatement(table, &statement)
		if result != ExecuteSuccess {
			t.Errorf("result must be execute success: %v", result)
		}
	}

	var i uint32 = 1
	var cursor *backend.Cursor = backend.CursorBegin(table)
	for !cursor.IsEndOfTable {
		var row backend.Row
		var readableRow backend.VisualRow

		rowSlotSlice := backend.CursorValue(cursor)
		backend.DeserializeRow(rowSlotSlice, &row)

		readableRow.PrimaryID = row.PrimaryID
		readableRow.UserName = util.ToString(row.UserName[:])
		readableRow.Email = util.ToString(row.Email[:])

		if readableRow.PrimaryID != i {
			t.Errorf("Primary key is must be %v", i)
		}

		i++
		backend.CursorNext(cursor)
	}

	backend.CloseDB(table)
	os.Remove(dbFile)
}

func TestFileLength(t *testing.T) {
	dbFile := "./FileLen.db"
	table := backend.OpenDB(dbFile)
	inputBuffer := cli.NewInputBuffer()
	InsertNum := uint32(10)
	for i := uint32(0); i < InsertNum; i++ {

		inputBuffer.Buffer = fmt.Sprintf("insert %d %s %s", i, util.RandString(8), util.RandString(8)+"@google.com")
		inputBuffer.BufLen = len(inputBuffer.Buffer)

		var statement Statement
		result := PrepareStatement(inputBuffer, &statement)

		if result != PrepareSuccess {
			t.Errorf("result must be success: %v", result)
		}

		result = RunStatement(table, &statement)
		if result != ExecuteSuccess {
			t.Errorf("result must be execute success: %v", result)
		}
	}

	backend.CloseDB(table)

	tableNew := backend.OpenDB(dbFile)
	RealFileLength := backend.NodeSize
	if tableNew.Pager.FileLength != int64(RealFileLength) {
		t.Errorf("file size must be %v, but it is %v", backend.NodeSize, RealFileLength)
	}

	backend.CloseDB(tableNew)
	os.Remove(dbFile)
}
