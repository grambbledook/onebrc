package main

import (
	"bufio"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type TestData struct {
	input  string
	output string
}

func Test_CalculateAggregates_Short(t *testing.T) {

	for _, test := range []TestData{
		{"measurements-1.txt", "measurements-1.out"},
		{"measurements-2.txt", "measurements-2.out"},
		{"measurements-3.txt", "measurements-3.out"},
		{"measurements-10.txt", "measurements-10.out"},
		{"measurements-20.txt", "measurements-20.out"},
		{"measurements-boundaries.txt", "measurements-boundaries.out"},
		{"measurements-complex-utf8.txt", "measurements-complex-utf8.out"},
		{"measurements-dot.txt", "measurements-dot.out"},
		{"measurements-short.txt", "measurements-short.out"},
		{"measurements-shortest.txt", "measurements-shortest.out"},
	} {
		executeTest(t, test)
	}
}

func Test_CalculateAggregates_Long(t *testing.T) {

	for _, test := range []TestData{
		{"measurements-10000-unique-keys.txt", "measurements-10000-unique-keys.out"},
	} {
		executeTest(t, test)
	}
}

func Test_CalculateAggregates_Rounding(t *testing.T) {

	for _, test := range []TestData{
		{"measurements-rounding.txt", "measurements-rounding.out"},
	} {
		executeTest(t, test)
	}
}

func executeTest(t *testing.T, test TestData) {
	out := os.Stdout
	defer func() { os.Stdout = out }()

	r, w, _ := os.Pipe()
	os.Stdout = w

	naive(path(test.input))

	result := actual(r)
	expected := expected(path(test.output))

	if ok := assert.Equal(t, expected, result); !ok {
		t.Errorf("Failed for file: [%s]", test.input)
	}
}

func path(name string) string {
	path, _ := filepath.Abs(filepath.Join("src/test/resources/samples", name))
	return path
}

func actual(r io.Reader) string {
	data, _ := bufio.NewReader(r).ReadString('}')
	return data
}

func expected(path string) string {
	data, _ := os.ReadFile(path)
	return strings.TrimSpace(string(data))
}
