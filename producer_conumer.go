package main

import (
	"bufio"
	"io"
	"os"
	"sync"
)

type ProducerConsumerTask struct {
	file       string
	bufferSize int
	_          struct{}
}

func (t ProducerConsumerTask) Name() string {
	return "Producer-Consumer Task"
}

func (t ProducerConsumerTask) File() string {
	return t.file
}

func (t ProducerConsumerTask) Execute() {
	data := bufio.NewReaderSize(Must(os.Open(t.file)), t.bufferSize)

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
