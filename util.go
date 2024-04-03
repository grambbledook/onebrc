package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

const delimiter = ";"

type ComputeConfig struct {
	file       string
	iterations int
	bufferSize int
	_          struct{}
}

type GenerateConfig struct {
	output       string
	records      int
	maxChunkSize int
	workers      int
	_            struct{}
}

func (c GenerateConfig) chunkSize() int {
	return min(c.records, c.maxChunkSize)
}

func (c GenerateConfig) totalChunks() int {
	return c.records / c.chunkSize()
}

type Aggregate struct {
	min   float32
	max   float32
	sum   float32
	count int
}

func try[T any](t T, err error) T {
	if err != nil {
		fmt.Printf("An error occured duting processing [%s]", err)
		panic(err)
	}
	return t
}

func parse(line string) (string, float32) {
	length := len(line)
	idx := strings.Index(line, delimiter)

	city := line[:idx]
	temperature := try(strconv.ParseFloat(line[idx+1:length-1], 32))

	return city, float32(temperature)
}

func render(cities []string, aggregates map[string]*Aggregate) {
	slices.Sort(cities)

	fmt.Print("{")
	for i, city := range cities {
		if i > 0 {
			fmt.Print(", ")
		}

		a := aggregates[city]
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			city,
			round(a.min),
			round(round(a.sum)/float32(a.count)),
			round(a.max),
		)
	}
	fmt.Println("}")
}

func round(mean float32) float32 {
	return float32(math.Round(float64(mean)*10) / 10)
}
