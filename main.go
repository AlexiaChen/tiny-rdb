package main

import (
	"fmt"
	"os"

	"tiny-rdb/front-end/cli"
)

func main() {
	inputBuffer := cli.NewInputBuffer()

	for {
		cli.PrintPrompt()
		cli.ReadInput(inputBuffer)

		if inputBuffer.Buffer == "exit" || inputBuffer.Buffer == "quit" {
			os.Exit(cli.ExitSuccess)
		} else {

			if inputBuffer.BufLen > 0 {
				fmt.Printf("Unrecognized command：%v\n", inputBuffer.Buffer)
			}
		}
	}
}
