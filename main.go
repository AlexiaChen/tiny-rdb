package main

import (
	"fmt"
	"log"
	"os"
	"tiny-rdb/backend"
	"tiny-rdb/frontend/cli"
	"tiny-rdb/frontend/sql"
	"tiny-rdb/util"
)

func initLog() {
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
}

func main() {

	if len(os.Args) < 2 {
		fmt.Printf("tiny-rdb [db-file]\n")
		os.Exit(util.ExitFailure)
	}

	initLog()
	inputBuffer := cli.NewInputBuffer()
	var table *backend.Table = backend.OpenDB(os.Args[1])
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
		case sql.ExecuteDuplicateKey:
			fmt.Println("Error: Duplicate Key")
		case sql.ExecuteTableFull:
			fmt.Println("Error: Table Full")
		case sql.ExecuteFail:
			fmt.Println("Unknown Error: Failed to execute")
		}

	}
}
