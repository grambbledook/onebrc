package main

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"strconv"
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

func readBytes(config ComputeConfig) int {
	data := bufio.NewReaderSize(try(os.Open(config.file)), config.bufferSize)

	lines := make(chan string)
	count := 0
	for {
		_, err := data.ReadBytes('\n')
		if err == io.EOF {
			close(lines)
			break
		}

		if err != nil {
			close(lines)
			panic(err)
		}

		count++
	}
	return count
}

func bufferParallel(config ComputeConfig) int {
	f := try(os.Stat(config.file))

	workers := runtime.NumCPU()
	size := int(f.Size()) / workers

	outputs := make([]chan string, workers)
	for i := 0; i < workers; i++ {
		outputs[i] = make(chan string)
	}

	for i := 0; i < workers; i++ {
		start, end := i*size, (i+1)*size
		go readChunk(config, start, end, outputs[i], i)
	}

	total := 0
	for _, ch := range outputs {
		count, _ := strconv.Atoi(<-ch)
		total += count
	}
	println("Total lines", total)
	return 0
}

func readChunk(config ComputeConfig, start, end int, out chan string, id int) {
	defer close(out)

	file := try(os.Open(config.file))
	defer file.Close()

	try(file.Seek(int64(start), io.SeekStart))

	println("Worker", id, "started", "total bytes to read", end-start)
	data := bufio.NewReaderSize(file, config.bufferSize)
	totalBytesRead := 0
	if id != 0 {
		line := try(data.ReadBytes('\n'))
		totalBytesRead += len(line)
	}

	count := 0
	for {
		line, err := data.ReadBytes('\n')
		if count == 0 {
			println("Worker", id, "first line", string(line))
		}

		if err == io.EOF || totalBytesRead-1 > end-start {
			println("Worker", id, "total bytes", totalBytesRead, "last line", string(line))
			out <- strconv.Itoa(count)
			break
		}

		if err != nil {
			panic(err)
		}

		count += 1
		totalBytesRead += len(line)
	}
}
