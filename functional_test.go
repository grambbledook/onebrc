package main

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type TaskFactory[T Task] struct{}

func (f TaskFactory[T]) Create() *T {
	var a T
	return &a
}

type TestSuite struct {
	suite.Suite
	taskProvider func(file string) Task
}

func TestNaive(t *testing.T) {
	testingSuite := new(TestSuite)

	testingSuite.taskProvider = func(path string) Task {
		task := TaskFactory[NaiveComputeTask]{}.Create()
		task.file = path
		return task
	}

	suite.Run(t, testingSuite)
}

func TestChain(t *testing.T) {
	testingSuite := new(TestSuite)

	testingSuite.taskProvider = func(path string) Task {
		task := TaskFactory[ProducerConsumerTask]{}.Create()
		task.file = path
		return task
	}

	suite.Run(t, testingSuite)
}

func TestParallel(t *testing.T) {
	testingSuite := new(TestSuite)

	testingSuite.taskProvider = func(path string) Task {
		task := TaskFactory[ParallelProducerConsumerTask]{}.Create()
		task.file = path
		return task
	}

	suite.Run(t, testingSuite)
}

func TestParallelStaged(t *testing.T) {
	testingSuite := new(TestSuite)

	testingSuite.taskProvider = func(path string) Task {
		task := TaskFactory[ParallelStagedProducerConsumerTask]{}.Create()
		task.file = path
		return task
	}

	suite.Run(t, testingSuite)
}

type TestData struct {
	input  string
	output string
}

func (s *TestSuite) Test_CalculateAggregates_Short() {

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
		s.executeTest(test)
	}
}

func (s *TestSuite) Test_CalculateAggregates_Long() {

	for _, test := range []TestData{
		{"measurements-10000-unique-keys.txt", "measurements-10000-unique-keys.out"},
	} {
		s.executeTest(test)
	}
}

func (s *TestSuite) Test_CalculateAggregates_Rounding() {

	for _, test := range []TestData{
		{"measurements-rounding.txt", "measurements-rounding.out"},
	} {
		s.executeTest(test)
	}
}

func (s *TestSuite) executeTest(test TestData) {
	out := os.Stdout
	defer func() { os.Stdout = out }()

	r, w, _ := os.Pipe()
	os.Stdout = w

	ch := make(chan string)
	go readFromPipe(r, ch)

	go s.taskProvider(path(test.input))

	result := actual(ch)
	expected := expected(path(test.output))

	if ok := assert.Equal(s.T(), expected, result); !ok {
		s.T().Errorf("Failed for file: [%s]", test.input)
	}
}

func path(name string) string {
	path, _ := filepath.Abs(filepath.Join("src/test/resources/samples", name))
	return path
}

func readFromPipe(r io.Reader, ch chan string) {
	for {
		buf := make([]byte, 1024)
		n := Must(r.Read(buf))
		if n == 0 {
			continue
		}

		s := string(buf[:n])
		ch <- strings.TrimSuffix(s, "\n")
		if bytes.IndexByte(buf, '}') != -1 {
			break
		}
	}
	close(ch)
}

func actual(ch chan string) string {
	result := bytes.Buffer{}
	for line := range ch {
		result.WriteString(line)
	}
	return result.String()
}

func expected(path string) string {
	data, _ := os.ReadFile(path)
	return strings.TrimSpace(string(data))
}
