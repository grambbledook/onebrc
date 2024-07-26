package main

import (
	"fmt"
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

func ParseFloat(line string) (string, float32) {
	length := len(line)
	idx := strings.Index(line, delimiter)

	city := line[:idx]
	temperature := Must(strconv.ParseFloat(line[idx+1:length-1], 32))

	return city, float32(temperature)
}

func ParseInt(line string) (string, int) {
	idx := strings.Index(line, delimiter)

	temperature, sign := 0, 1
	city := line[:idx]
	for _, c := range line[idx+1:] {
		if c == '\n' {
			break
		}

		if c == '-' {
			sign *= -1
			continue
		}

		if c == '.' {
			continue
		}
		temperature = temperature*10 + int(c-'0')
	}

	return city, temperature * sign
}

func render[T Number](cities []string, aggregates map[string]*Aggregate[T]) {
	slices.Sort(cities)

	fmt.Print("{")
	for i, city := range cities {
		if i > 0 {
			fmt.Print(", ")
		}
		a := aggregates[city]
		fmt.Printf("%s=%s/%s/%s",
			city,
			a.Min(),
			a.Avg(),
			a.Max(),
		)
	}
	fmt.Println("}")
}
