package main

import (
	"bufio"
	"io"
	"os"
	"strings"
)

func buffer(config ComputeConfig) int {
	data := bufio.NewReaderSize(try(os.Open(config.file)), config.bufferSize)

	lines := make(chan string)
	count := 0
	for {
		line, err := data.ReadString('\n')
		if err == io.EOF {
			close(lines)
			break
		}

		if err != nil {
			close(lines)
			panic(err)
		}

		if strings.HasPrefix(line, "#") {
			continue
		}

		count++
	}
	return count
}
