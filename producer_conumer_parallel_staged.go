package main

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"sync"
)

func pcpStaged(config ComputeConfig) {
	file := try(os.Stat(config.file))

	workers := runtime.NumCPU() / 2
	chunkSize := int(file.Size()) / workers

	aggregators := sync.WaitGroup{}
	aggregators.Add(workers)

	aggregates := make(chan map[string]*Aggregate, workers)

	for i := 0; i < workers; i++ {
		buffer := make(chan string, 500)

		start, end := i*chunkSize, (i+1)*chunkSize
		if i == workers-1 {
			end = int(file.Size())
		}
		go reader2(i, config, start, end, buffer)
		go aggregator(buffer, aggregates, &aggregators)
	}

	reducers := sync.WaitGroup{}
	reducers.Add(1)
	go reduce(aggregates, &reducers)

	aggregators.Wait()
	close(aggregates)

	reducers.Wait()
}

func reader2(id int, config ComputeConfig, start, end int, buffer chan string) {
	defer close(buffer)

	file := try(os.Open(config.file))
	try(file.Seek(int64(start), io.SeekStart))
	defer file.Close()

	in := bufio.NewReaderSize(file, config.bufferSize)

	totalBytes := 0
	if id != 0 {
		line := try(in.ReadBytes('\n'))
		totalBytes += len(line)
	}

	for totalBytes <= end-start {
		line, err := in.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		totalBytes += len(line)

		buffer <- line
	}
}

func aggregator(buffer chan string, aggregates chan map[string]*Aggregate, wg *sync.WaitGroup) {
	defer wg.Done()

	cities := make(map[string]*Aggregate)

	for line := range buffer {
		city, temperature := parse(line)
		aggregate, ok := cities[city]
		if !ok {
			aggregate = &Aggregate{temperature, temperature, temperature, 1}
			cities[city] = aggregate

			continue
		}

		aggregate.min = min(aggregate.min, temperature)
		aggregate.max = max(aggregate.max, temperature)
		aggregate.sum += temperature
		aggregate.count += 1
	}
	aggregates <- cities
}
