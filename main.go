package main

import (
	"fmt"
	"tiny-rdb/back-end/table"
	"tiny-rdb/front-end/cli"
	"tiny-rdb/front-end/sql"
)

func main() {
	inputBuffer := cli.NewInputBuffer()
	var table *table.Table = new(table.Table)
	for {
		cli.PrintPrompt()
		cli.ReadInput(inputBuffer)

		if inputBuffer.BufLen == 0 {
			continue
		}

		if cli.IsRawCommand(&(inputBuffer.Buffer)) {
			switch sql.RunRawCommand(inputBuffer) {
			case sql.RawCommandSuccess:
				continue
			case sql.RawCommandUnrecognizedCMD:
				fmt.Printf("Unrecognized raw command: %v\n", inputBuffer.Buffer)
				continue
			}
		}

		var statement sql.Statement
		switch sql.PrepareStatement(inputBuffer, &statement) {
		case sql.PrepareSuccess:
			break
		case sql.PrepareStringTooLong:
			fmt.Errorf("String too long")
			continue
		case sql.PrepareSyntaxError:
			fmt.Errorf("Syntax Error: Cannot parse statement")
			continue
		case sql.PrepareUnrecognizedStatement:
			fmt.Errorf("Unrecognized statement: %v", inputBuffer.Buffer)
			continue
		}

		switch sql.RunStatement(table, &statement) {
		case sql.ExecuteSuccess:
			fmt.Println("Executed statement.")
		case sql.ExecuteTableFull:
			fmt.Errorf("Error: Table Full")
		case sql.ExecuteFail:
			fmt.Errorf("Unknown Error: Failed to execute")
		}

	}
}
