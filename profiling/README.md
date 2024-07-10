### System:

The tests were run in 5 iterations on the same system using the same file.

| CPU   | Ryzen 5700x3d            |
|-------|--------------------------|
| RAM   | 32GB DDR4 3200           |
| SSD   | Samsung 970 Evo Plus 2TB |
| OS    | Windows 10 22H2          |
| Input | 13,7 GB                  |

### Cpu profiling with pprof

```
go tool pprof -http=":8080" pprofbin cpu.prof
```

### Baseline implementation
[naive](naive.md)

### Parallel implementation
[parallel](parallel.md)

### Staged implementation
[staged](staged.md)