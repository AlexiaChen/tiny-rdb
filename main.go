package main

import (
	"fmt"
	"os"

	"tiny-rdb/front-end/cli"
	"tiny-rdb/util"
)

func main() {
	inputBuffer := cli.NewInputBuffer()

	for {
		cli.PrintPrompt()
		cli.ReadInput(inputBuffer)

		if inputBuffer.Buffer == "exit" || inputBuffer.Buffer == "quit" {
			os.Exit(util.ExitSuccess)
		} else {

			if inputBuffer.BufLen > 0 {
				fmt.Printf("Unrecognized command：%v\n", inputBuffer.Buffer)
			}
		}
	}
}
