package main

import (
	"fmt"
	"tiny-rdb/front-end/cli"
	"tiny-rdb/front-end/sql"
)

func main() {
	inputBuffer := cli.NewInputBuffer()

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
		case sql.PrepareUnrecognizedStatement:
			fmt.Printf("Unrecognized statement: %v\n", inputBuffer.Buffer)
			continue
		}

		sql.RunStatement(&statement)
		fmt.Println("Executed statement.")
	}
}
