package beauty

import (
	"fmt"
	"math"
)

type Summary struct {
	min   int
	max   int
	sum   int
	count int
}

func NewSummary() Summary {
	return Summary{
		min:   math.MaxInt32,
		max:   math.MinInt32,
		sum:   0,
		count: 0,
	}
}

func (s *Summary) Record(value int) {
	s.count++
	s.sum += value
	s.min = min(s.min, value)
	s.max = max(s.max, value)
}

func (s *Summary) Summary() string {
	return fmt.Sprintf("Min: [%d ms], Max: [%d ms], Avg: [%d ms]\n", s.min, s.max, s.sum/s.count)
}
