package main

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"sync"
)

type ParallelStagedProducerConsumerTask[T Number] struct {
	file       string
	bufferSize int
	lineParser func(string) (string, T)
	_          struct{}
}

func (t ParallelStagedProducerConsumerTask[T]) Name() string {
	return "Parallel Staged Producer-Consumer Task"
}

func (t ParallelStagedProducerConsumerTask[T]) File() string {
	return t.file
}

func (t ParallelStagedProducerConsumerTask[T]) Execute() {
	file := Must(os.Stat(t.file))

	workers := runtime.NumCPU() / 2
	chunkSize := int(file.Size()) / workers

	aggregators := sync.WaitGroup{}
	aggregators.Add(workers)

	aggregates := make(chan map[string]*Aggregate[T], 1024)

	for i := 0; i < workers; i++ {
		buffer := make(chan string, workers)

		start, end := i*chunkSize, (i+1)*chunkSize
		if i == workers-1 {
			end = int(file.Size())
		}
		go t.Reader(i, start, end, buffer)
		go t.Aggregator(buffer, aggregates, &aggregators)
	}

	reducers := sync.WaitGroup{}
	reducers.Add(1)
	go t.Reduce(aggregates, &reducers)

	aggregators.Wait()
	close(aggregates)

	reducers.Wait()
}

func (t ParallelStagedProducerConsumerTask[T]) Reader(id int, start, end int, buffer chan string) {
	defer close(buffer)

	file := Must(os.Open(t.file))
	Must(file.Seek(int64(start), io.SeekStart))
	defer file.Close()

	in := bufio.NewReaderSize(file, t.bufferSize)

	totalBytes := 0
	if id != 0 {
		line := Must(in.ReadBytes('\n'))
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

func (t ParallelStagedProducerConsumerTask[T]) Aggregator(buffer chan string, aggregates chan map[string]*Aggregate[T], wg *sync.WaitGroup) {
	defer wg.Done()

	cities := make(map[string]*Aggregate[T])

	for line := range buffer {
		city, temperature := t.lineParser(line)
		aggregate, ok := cities[city]
		if !ok {
			aggregate = &Aggregate[T]{temperature, temperature, temperature, 1}
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

func (t ParallelStagedProducerConsumerTask[T]) Reduce(input chan map[string]*Aggregate[T], wg *sync.WaitGroup) {
	defer wg.Done()

	cities := make([]string, 0)
	aggregates := make(map[string]*Aggregate[T])

	for chunk := range input {
		for city, other := range chunk {
			this, ok := aggregates[city]
			if !ok {
				cities = append(cities, city)
				aggregates[city] = other
				continue
			}
			this.max = max(this.max, other.max)
			this.min = min(this.min, other.min)
			this.sum += other.sum
			this.count += other.count
		}
	}

	render(cities, aggregates)
}
