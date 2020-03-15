package sql

import (
	"fmt"
	"os"
	"testing"
	tablePackage "tiny-rdb/back-end/table"
	"tiny-rdb/front-end/cli"
	"tiny-rdb/util"
)

func TestRunRawCommand(t *testing.T) {
	inputBuffer := cli.NewInputBuffer()
	inputBuffer.Buffer = "testCmd"
	inputBuffer.BufLen = len(inputBuffer.Buffer)
	dbFile := "./RawCmd.db"
	table := tablePackage.OpenDB(dbFile)
	if RunRawCommand(inputBuffer, table) != RawCommandUnrecognizedCMD {
		t.Errorf("Command is not unrecognized command")
	}

	inputBuffer.Buffer = "#other"
	inputBuffer.BufLen = len(inputBuffer.Buffer)

	if RunRawCommand(inputBuffer, table) != RawCommandSuccess {
		t.Errorf("Command is not success command")
	}

	tablePackage.CloseDB(table)
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
	table := tablePackage.OpenDB(dbFile)
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

	var cursor *tablePackage.Cursor = tablePackage.CursorBegin(table)
	for !cursor.IsEndOfTable {
		var row tablePackage.Row
		var readableRow tablePackage.VisualRow

		rowSlotSlice := tablePackage.CursorValue(cursor)
		rowSize := tablePackage.DeserializeRow(&rowSlotSlice, &row)
		if rowSize != tablePackage.RowSize {
			t.Errorf("Row Size Error: %v", rowSize)
		}

		readableRow.PrimaryID = row.PrimaryID
		readableRow.UserName = util.ToString(row.UserName[:])
		readableRow.Email = util.ToString(row.Email[:])

		if readableRow.PrimaryID != 12 || readableRow.UserName != "chen" || readableRow.Email != "we@qq.com" {
			t.Errorf("Row (%v, %s, %s) Error", readableRow.PrimaryID, readableRow.UserName, readableRow.Email)
		}

		tablePackage.CursorNext(cursor)
	}

	tablePackage.CloseDB(table)
	os.Remove(dbFile)

}

func TestBunchOfInsert(t *testing.T) {
	dbFile := "./BunchOfInsert.db"
	table := tablePackage.OpenDB(dbFile)
	inputBuffer := cli.NewInputBuffer()
	InsertNum := uint32(500)
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

	if table.NumRows != InsertNum {
		t.Errorf("Row Num must be %v, but it is %v", InsertNum, table.NumRows)
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

	tablePackage.CloseDB(table)

	tableNew := tablePackage.OpenDB(dbFile)

	if tableNew.NumRows != InsertNum {
		t.Errorf("Row Num must be %v, but it is %v", InsertNum, tableNew.NumRows)
	}

	tablePackage.CloseDB(tableNew)
	os.Remove(dbFile)
}

func TestFileLength(t *testing.T) {
	dbFile := "./FileLen.db"
	table := tablePackage.OpenDB(dbFile)
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

	tablePackage.CloseDB(table)

	tableNew := tablePackage.OpenDB(dbFile)
	RealFileLenght := (tableNew.NumRows/tablePackage.RowsPerPage)*tablePackage.PageSize + (table.NumRows%tablePackage.RowsPerPage)*tablePackage.RowSize
	if tableNew.Pager.FileLength != int64(RealFileLenght) {
		t.Errorf("%v rows size must be %v, but it is %v", tableNew.NumRows, RealFileLenght, tableNew.Pager.FileLength)
	}

	tablePackage.CloseDB(tableNew)
	os.Remove(dbFile)
}
