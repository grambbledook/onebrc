package main

import (
	"fmt"
	"math"
	"strconv"
)

type Number interface {
	int | float32
}

type Aggregate[T Number] struct {
	min   T
	max   T
	sum   T
	count int
}

func (a *Aggregate[T]) Min() string {
	switch minn := any(a.min).(type) {
	case int:
		return toString(minn/10, minn%10)
	default:
		return fmt.Sprintf("%.1f", minn)
	}
}

func (a *Aggregate[T]) Max() string {
	switch maxx := any(a.max).(type) {
	case int:
		return toString(maxx/10, maxx%10)
	default:

		return fmt.Sprintf("%.1f", maxx)
	}
}

func (a *Aggregate[T]) Avg() string {
	switch sum := any(a.sum).(type) {
	case int:
		avg := sum * 10 / a.count
		if avg%10 >= 5 {
			avg += 10 - avg%10
		}

		return toString(avg/100, avg%100/10)
	case float32:
		avg := round(sum) / float32(a.count)
		return fmt.Sprintf("%.1f", round(avg))
	}
	panic("unreachable")
}

func toString(a, b int) string {
	if a == 0 && b < 0 {
		return "-" + strconv.Itoa(a) + "." + strconv.Itoa(abs(b))
	}
	return strconv.Itoa(a) + "." + strconv.Itoa(abs(b))
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func round(a float32) float32 {
	return float32(math.Round(float64(a)*10) / 10)
}
