package sql

import (
	"fmt"
	"os"
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
)

// StatementType type of statement
type StatementType = int

// PrepareStatementResult result of statement
type PrepareStatementResult = int

// RawCommandResult result of raw command
type RawCommandResult = int

// Statement represent a statment
type Statement struct {
	Type        StatementType
	RowToInsert Row
	RowToDelete Row
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
	if inputBuffer.Buffer == "insert" {
		statement.Type = InsertStatement
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
func RunStatement(statement *Statement) {
	switch statement.Type {
	case InsertStatement:
		// TODO: Insert
	case SelectStatement:
		// TODO: Select
	case DeleteStatement:
		// TODO: Delete
	case CreateStatement:
		// TODO: Create
	default:
		fmt.Println("Unkown Statement.")
	}
}
