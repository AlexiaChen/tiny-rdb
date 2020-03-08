package main

import (
	"fmt"
	"log"
	"os"
	"tiny-rdb/back-end/table"
	"tiny-rdb/front-end/cli"
	"tiny-rdb/front-end/sql"
	"tiny-rdb/util"
)

func initLog() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
}

func main() {

	if len(os.Args) < 2 {
		fmt.Printf("tiny-rdb [db-file]")
		os.Exit(util.ExitFailure)
	}

	initLog()
	inputBuffer := cli.NewInputBuffer()
	var table *table.Table = table.OpenDB(os.Args[1])
	for {
		cli.PrintPrompt()
		cli.ReadInput(inputBuffer)

		if inputBuffer.BufLen == 0 {
			continue
		}

		if cli.IsRawCommand(&(inputBuffer.Buffer)) {
			switch sql.RunRawCommand(inputBuffer, table) {
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
			fmt.Printf("String too long")
			continue
		case sql.PrepareSyntaxError:
			fmt.Printf("Syntax Error: Cannot parse statement")
			continue
		case sql.PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized statement: %v", inputBuffer.Buffer)
			continue
		}

		switch sql.RunStatement(table, &statement) {
		case sql.ExecuteSuccess:
			fmt.Println("Executed statement.")
		case sql.ExecuteTableFull:
			fmt.Println("Error: Table Full")
		case sql.ExecuteFail:
			fmt.Println("Unknown Error: Failed to execute")
		}

	}
}
