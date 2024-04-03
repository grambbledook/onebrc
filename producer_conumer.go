package main

import (
	"bufio"
	"io"
	"os"
	"strings"
	"sync"
)

func pc(config ComputeConfig) {
	data := bufio.NewReaderSize(try(os.Open(config.file)), config.bufferSize)

	cities := make([]string, 0)
	aggregates := make(map[string]*Aggregate)

	lines := make(chan string)

	go func() {
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

			lines <- line
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		for line := range lines {
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
		wg.Done()
	}()

	wg.Wait()
	render(cities, aggregates)
}
