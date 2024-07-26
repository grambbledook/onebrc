package main

import (
	"bufio"
	"io"
	"os"
	"runtime"
	"sync"
)

type ParallelProducerConsumerTask[T Number] struct {
	file       string
	bufferSize int
	lineParser func(string) (string, T)
	_          struct{}
}

func (t ParallelProducerConsumerTask[T]) Name() string {
	return "Parallel Producer-Consumer Task"
}

func (t ParallelProducerConsumerTask[T]) File() string {
	return t.file
}

func (t ParallelProducerConsumerTask[T]) Execute() {
	file := Must(os.Stat(t.file))

	workers := runtime.NumCPU()
	chunkSize := int(file.Size()) / workers

	readers := sync.WaitGroup{}
	readers.Add(workers)

	aggregates := make(chan map[string]*Aggregate[T], workers)
	for i := 0; i < workers; i++ {

		start, end := i*chunkSize, (i+1)*chunkSize
		if i == workers-1 {
			end = int(file.Size())
		}
		go t.Reader(i, start, end, aggregates, &readers)
	}

	reducers := sync.WaitGroup{}
	reducers.Add(1)
	go t.Reduce(aggregates, &reducers)

	readers.Wait()
	close(aggregates)

	reducers.Wait()
}

func (t ParallelProducerConsumerTask[T]) Reader(
	id int,
	start, end int,
	aggregates chan map[string]*Aggregate[T],
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	file := Must(os.Open(t.file))
	Must(file.Seek(int64(start), io.SeekStart))
	defer file.Close()

	in := bufio.NewReaderSize(file, t.bufferSize)

	totalBytes := 0
	if id != 0 {
		line := Must(in.ReadBytes('\n'))
		totalBytes += len(line)
	}

	cities := make(map[string]*Aggregate[T])
	total := 0
	for totalBytes <= end-start {
		line, err := in.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		totalBytes += len(line)
		total++

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

func (t ParallelProducerConsumerTask[T]) Reduce(input chan map[string]*Aggregate[T], wg *sync.WaitGroup) {
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
