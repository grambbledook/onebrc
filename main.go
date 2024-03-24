package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const delimiter = ";"

type Aggregate struct {
	min   float32
	max   float32
	sum   float32
	count int
}

func do[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func main() {
	path := do(filepath.Abs("data/weather_stations.csv"))
	naive(path)
}

func naive(path string) {
	data := bufio.NewReader(do(os.Open(path)))

	cities := make([]string, 0)
	aggregates := make(map[string]*Aggregate, 0)

	for {
		line, err := data.ReadString('\n')
		if err == io.EOF {
			break
		}
		if strings.HasPrefix(line, "#") {
			continue
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

func parse(line string) (string, float32) {
	length := len(line)
	idx := strings.Index(line, delimiter)

	city := line[:idx]
	temperature := do(strconv.ParseFloat(line[idx+1:length-1], 32))

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
	fmt.Print("}")
}

func round(mean float32) float32 {
	return float32(math.Round(float64(mean)*10) / 10)
}
