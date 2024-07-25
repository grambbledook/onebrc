package main

import (
	"bufio"
	"io"
	"os"
)

type NaiveComputeTask struct {
	file       string
	bufferSize int
	_          struct{}
}

func (t NaiveComputeTask) Name() string {
	return "Naive"
}

func (t NaiveComputeTask) File() string {
	return t.file
}

func (t NaiveComputeTask) Execute() {
	data := bufio.NewReaderSize(Must(os.Open(t.file)), t.bufferSize)

	cities := make([]string, 0)
	aggregates := make(map[string]*Aggregate)

	for {
		line, err := data.ReadString('\n')
		if err == io.EOF {
			break
		}

		city, temperature := parse(line)

		aggregate, ok := aggregates[city]
		if !ok {
			cities = append(cities, city)

			aggregate = &Aggregate{temperature, temperature, temperature, 1}
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
