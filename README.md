# 1️⃣🐝🏎️ The One Billion Row Challenge

This repo is based on  Gunnar Morling's [The One Billion Rows Challenge](https://github.com/gunnarmorling/1brc). 

The goal is to explore Golang standard library and profiling utils while working with a large dataset.

## Build
```
go build -o onebrc
```

## Generate data
```
Usage:
  obebrc generate [flags]

Flags:
  -h, --help            help for generate
  -o, --output string   output file (default "measurements.csv")
  -r, --records int     number of records to generate (default 100)
  -s, --size int        size of the chunk to generate (default 1)
  -w, --workers int     number of workers (default 1)

Global Flags:
      --p                        enable cpu profiling
      --profiler_output string   cpu profiler output file (default "cpu.prof")
```

```
onebrc generate -o measurements.csv -r 1000000000  -w 50 -s 1000000
```

## Process measurements
```
Usage:
  obebrc generate [flags]

Modes:
  naive       A naive implementation of 1brc
  parallel    A parallel producer-consumer implementation of 1brc
  sequential  A producer-consumer implementation of 1brc
  staged      A parallel staged producer-consumer implementation of 1brc

Flags:
  -b, --buffer int               buffer size for the buffered reader (default 1024)
  -f, --file string              input file (default "measurements.csv")
  -n, --iterations int           number of iterations to run the computation (default 1)
  
Global Flags:
      --p                        enable cpu profiling
      --profiler_output string   cpu profiler output file (default "cpu.prof")
```

```
onebrc compute parallel -f measurements.csv -n 5
```

