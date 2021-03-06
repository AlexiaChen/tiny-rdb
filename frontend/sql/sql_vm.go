package sql

import (
	"fmt"
	"os"
	"strings"
	"tiny-rdb/backend"
	"tiny-rdb/frontend/cli"
	"tiny-rdb/util"
)

// const Result var
const (
	// Raw Command Result
	RawCommandSuccess         = iota
	RawCommandUnrecognizedCMD = iota

	// Prepare Statement Reuslt
	PrepareSuccess               = iota
	PrepareStringTooLong         = iota
	PrepareSyntaxError           = iota
	PrepareUnrecognizedStatement = iota

	// Satement Type
	InsertStatement = iota
	SelectStatement = iota
	DeleteStatement = iota
	CreateStatement = iota

	// Execute Result
	ExecuteSuccess      = iota
	ExecuteTableFull    = iota
	ExecuteDuplicateKey = iota
	ExecuteFail         = iota
)

// StatementType type of statement
type StatementType = int

// PrepareStatementResult result of statement
type PrepareStatementResult = int

// RawCommandResult result of raw command
type RawCommandResult = int

// ExecuteResult result of executed statment
type ExecuteResult = int

// Statement represent a statment
type Statement struct {
	Type        StatementType
	RowToInsert backend.Row
	RowToDelete backend.Row
}

// RunRawCommand Run raw command
func RunRawCommand(inputBuffer *cli.InputBuffer, table *backend.Table) RawCommandResult {
	if inputBuffer.Buffer == "#exit" || inputBuffer.Buffer == "#quit" {
		backend.CloseDB(table)
		os.Exit(util.ExitSuccess)
	}
	if inputBuffer.Buffer == "#other" {
		return RawCommandSuccess
	}
	if inputBuffer.Buffer == "#btree" {
		fmt.Println("Visual B-Tree:")
		backend.PrintTree(table.Pager, table.RootPageNum, 0)
		return RawCommandSuccess
	}
	return RawCommandUnrecognizedCMD
}

func prepareInsert(inputBuffer *cli.InputBuffer, statement *Statement) PrepareStatementResult {
	statement.Type = InsertStatement
	var UserName string
	var Email string
	argsParsed, err := fmt.Sscanf(inputBuffer.Buffer, "insert %d %s %s", &statement.RowToInsert.PrimaryID, &UserName, &Email)
	if err != nil {
		return PrepareSyntaxError
	}

	if argsParsed != 3 {
		return PrepareSyntaxError
	}

	if len(UserName) > backend.UserNameSize || len(Email) > backend.EmailSize {
		return PrepareStringTooLong
	}

	copy(statement.RowToInsert.UserName[:], UserName)
	copy(statement.RowToInsert.Email[:], Email)

	return PrepareSuccess
}

// PrepareStatement Prepare statement
func PrepareStatement(inputBuffer *cli.InputBuffer, statement *Statement) PrepareStatementResult {
	if strings.HasPrefix(inputBuffer.Buffer, "insert") {
		return prepareInsert(inputBuffer, statement)
	}

	if inputBuffer.Buffer == "select" {
		statement.Type = SelectStatement
		return PrepareSuccess
	}

	if inputBuffer.Buffer == "delete" {
		statement.Type = DeleteStatement
		return PrepareSuccess
	}

	if inputBuffer.Buffer == "create" {
		statement.Type = CreateStatement
		return PrepareSuccess
	}

	return PrepareUnrecognizedStatement
}

// RunStatement Run statement
func RunStatement(table *backend.Table, statement *Statement) ExecuteResult {
	switch statement.Type {
	case InsertStatement:
		return RunInsert(table, statement)
	case SelectStatement:
		return RunSelect(table, statement)
	case DeleteStatement:
		// TODO: Delete
	case CreateStatement:
		// TODO: Create
	default:
		fmt.Println("Unkown Statement.")
	}
	return ExecuteFail
}

// RunInsert run insert statment
func RunInsert(table *backend.Table, statement *Statement) ExecuteResult {

	var key uint32 = statement.RowToInsert.PrimaryID
	var cursor *backend.Cursor = backend.Find(table, key)

	var page *backend.Page = backend.GetPage(table.Pager, cursor.PageNum)
	var numCells uint32 = *backend.LeafNodeNumCells(page.Mem[:])

	if cursor.CellNum < numCells {
		var keyFinded uint32 = *backend.LeafNodeKey(page.Mem[:], cursor.CellNum)
		if key == keyFinded {
			return ExecuteDuplicateKey
		}
	}

	backend.InsertLeafNode(cursor, statement.RowToInsert.PrimaryID, &statement.RowToInsert)

	return ExecuteSuccess
}

// RunSelect run select statment
func RunSelect(table *backend.Table, statement *Statement) ExecuteResult {
	var cursor *backend.Cursor = backend.CursorBegin(table)
	for !cursor.IsEndOfTable {
		var row backend.Row
		var readableRow backend.VisualRow

		rowSlotSlice := backend.CursorValue(cursor)
		backend.DeserializeRow(rowSlotSlice, &row)

		readableRow.PrimaryID = row.PrimaryID
		readableRow.UserName = util.ToString(row.UserName[:])
		readableRow.Email = util.ToString(row.Email[:])

		backend.PrintRow(&readableRow)

		backend.CursorNext(cursor)
	}

	return ExecuteSuccess
}
