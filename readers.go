package main

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type BufferedReaderTask struct {
	file       string
	bufferSize int
	_          struct{}
}

func (t BufferedReaderTask) Name() string {
	return "Buffered Reader Task"
}

func (t BufferedReaderTask) File() string {
	return t.file
}

func (t BufferedReaderTask) Execute() {
	data := bufio.NewReaderSize(Must(os.Open(t.file)), t.bufferSize)

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
}

type BufferedReaderBytesTask struct {
	file       string
	bufferSize int
	_          struct{}
}

func (t BufferedReaderBytesTask) Name() string {
	return "Buffered Reader Bytes Task"
}

func (t BufferedReaderBytesTask) File() string {
	return t.file
}

func (t BufferedReaderBytesTask) Execute() {
	data := bufio.NewReaderSize(Must(os.Open(t.file)), t.bufferSize)

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
}

type ParallelBufferedReaderTask struct {
	file       string
	bufferSize int
	_          struct{}
}

func (t ParallelBufferedReaderTask) Name() string {
	return "Buffered Reader Task"
}

func (t ParallelBufferedReaderTask) File() string {
	return t.file
}

func (t ParallelBufferedReaderTask) Execute() {
	f := Must(os.Stat(t.file))

	workers := runtime.NumCPU()
	size := int(f.Size()) / workers

	outputs := make([]chan string, workers)
	for i := 0; i < workers; i++ {
		outputs[i] = make(chan string)
	}

	for i := 0; i < workers; i++ {
		start, end := i*size, (i+1)*size
		go t.ReadChunk(start, end, outputs[i], i)
	}

	total := 0
	for _, ch := range outputs {
		count, _ := strconv.Atoi(<-ch)
		total += count
	}
	println("Total lines", total)
}

func (t ParallelBufferedReaderTask) ReadChunk(start, end int, out chan string, id int) {
	defer close(out)

	file := Must(os.Open(t.file))
	defer file.Close()

	Must(file.Seek(int64(start), io.SeekStart))

	println("Worker", id, "started", "total bytes to Compute", end-start)
	data := bufio.NewReaderSize(file, t.bufferSize)
	totalBytesRead := 0
	if id != 0 {
		line := Must(data.ReadBytes('\n'))
		totalBytesRead += len(line)
	}

	count := 0
	for {
		line, err := data.ReadBytes('\n')

		if err == io.EOF || totalBytesRead > end-start {
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
