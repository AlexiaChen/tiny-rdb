package sql

import (
	"fmt"
	"os"
	"strings"
	tablePackage "tiny-rdb/back-end/table"
	"tiny-rdb/front-end/cli"
	"tiny-rdb/util"
)

// const Result var
const (
	// Raw Command Result
	RawCommandSuccess         = iota
	RawCommandUnrecognizedCMD = iota

	// Prepare Statement Reuslt
	PrepareSuccess               = iota
	PrepareSyntaxError           = iota
	PrepareUnrecognizedStatement = iota

	// Satement Type
	InsertStatement = iota
	SelectStatement = iota
	DeleteStatement = iota
	CreateStatement = iota

	// Execute Result
	ExecuteSuccess   = iota
	ExecuteTableFull = iota
	ExecuteFail      = iota
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
	RowToInsert tablePackage.Row
	RowToDelete tablePackage.Row
}

// RunRawCommand Run raw command
func RunRawCommand(inputBuffer *cli.InputBuffer) RawCommandResult {
	if inputBuffer.Buffer == "#exit" || inputBuffer.Buffer == "#quit" {
		os.Exit(util.ExitSuccess)
	}
	if inputBuffer.Buffer == "#other" {
		return RawCommandSuccess
	}
	return RawCommandUnrecognizedCMD
}

// PrepareStatement Prepare statement
func PrepareStatement(inputBuffer *cli.InputBuffer, statement *Statement) PrepareStatementResult {
	if strings.HasPrefix(inputBuffer.Buffer, "insert") {
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

		copy(statement.RowToInsert.UserName[:], UserName)
		copy(statement.RowToInsert.Email[:], Email)

		return PrepareSuccess
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
func RunStatement(table *tablePackage.Table, statement *Statement) ExecuteResult {
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
func RunInsert(table *tablePackage.Table, statement *Statement) ExecuteResult {
	if table.NumRows >= tablePackage.TableMaxRows {
		return ExecuteTableFull
	}

	rowSlotSlice := tablePackage.RowSlot(table, table.NumRows)
	tablePackage.SerializeRow(&statement.RowToInsert, &rowSlotSlice)
	table.NumRows = table.NumRows + 1
	return ExecuteSuccess
}

// RunSelect run select statment
func RunSelect(table *tablePackage.Table, statement *Statement) ExecuteResult {
	for i := uint32(0); i < table.NumRows; i++ {
		var row tablePackage.Row
		var readableRow tablePackage.VisualRow

		rowSlotSlice := tablePackage.RowSlot(table, i)
		tablePackage.DeserializeRow(&rowSlotSlice, &row)

		readableRow.PrimaryID = row.PrimaryID
		readableRow.UserName = util.ToString(row.UserName[:])
		readableRow.Email = util.ToString(row.Email[:])

		tablePackage.PrintRow(&readableRow)
	}
	return ExecuteSuccess
}
