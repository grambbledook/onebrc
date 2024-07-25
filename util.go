package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

const delimiter = ";"

func Cleanup(resource Closable) {
	err := resource.Close()
	if err != nil {
		fmt.Printf("An error occured during processing [%s]", err)
		panic(err)
	}
}
func Must[T any](t T, err error) T {
	if err != nil {
		fmt.Printf("An error occured during processing [%s]", err)
		panic(err)
	}
	return t
}

func parse(line string) (string, float32) {
	length := len(line)
	idx := strings.Index(line, delimiter)

	city := line[:idx]
	temperature := Must(strconv.ParseFloat(line[idx+1:length-1], 32))

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
