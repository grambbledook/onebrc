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
  -s, --size int        size of the chunk to generate (default 1000000)
  -w, --workers int     number of workers (default 1)
```


```
onebrc generate -o measurements.csv -r 1000000000  -w 50 -s 1000000
```

