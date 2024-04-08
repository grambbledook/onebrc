package main

import (
	"bufio"
	"io"
	"os"
)

func naive(config ComputeConfig) {
	data := bufio.NewReaderSize(try(os.Open(config.file)), config.bufferSize)

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
