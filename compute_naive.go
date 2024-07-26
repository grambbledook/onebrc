package main

import (
	"bufio"
	"io"
	"os"
)

type NaiveComputeTask[T Number] struct {
	file       string
	bufferSize int
	lineParser func(string) (string, T)
	_          struct{}
}

func (t NaiveComputeTask[T]) Name() string {
	return "Naive"
}

func (t NaiveComputeTask[T]) File() string {
	return t.file
}

func (t NaiveComputeTask[T]) Execute() {
	data := bufio.NewReaderSize(Must(os.Open(t.file)), t.bufferSize)

	cities := make([]string, 0)
	aggregates := make(map[string]*Aggregate[T])

	for {
		line, err := data.ReadString('\n')
		if err == io.EOF {
			break
		}

		city, temperature := t.lineParser(line)

		aggregate, ok := aggregates[city]
		if !ok {
			cities = append(cities, city)

			aggregate = &Aggregate[T]{temperature, temperature, temperature, 1}
			aggregates[city] = aggregate
			continue
		}

		aggregate.min = min(aggregate.min, temperature)
		aggregate.max = max(aggregate.max, temperature)
		aggregate.sum += temperature
		aggregate.count += 1
	}

	render(cities, aggregates)
}
