package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"onebrc/beauty"
	"os"
	"runtime/pprof"
	"time"
)

var (
	rootCmd = &cobra.Command{
		Use: "obebrc",
		Run: printCommands,
	}

	generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate random measurements",
		Run:   generate,
	}

	computeCmd = &cobra.Command{
		Use:   "compute",
		Short: "Process measurements",
		Run:   printCommands,
	}

	readCmd = &cobra.Command{
		Use:   "read",
		Short: "Read measurements file",
		Run:   printCommands,
	}

	readBufferCmd = &cobra.Command{
		Use:   "buffer",
		Short: "Read measurements file using buffered reader",
		Run:   read(func(config ComputeConfig) { buffer(config) }),
	}

	readBytesCmd = &cobra.Command{
		Use:   "bytes",
		Short: "Read measurements file using buffered reader",
		Run:   read(func(config ComputeConfig) { readBytes(config) }),
	}

	readParallelCmd = &cobra.Command{
		Use:   "parallel",
		Short: "Read measurements file using buffered reader",
		Run:   read(func(config ComputeConfig) { bufferParallel(config) }),
	}

	computeNaiveCmd = &cobra.Command{
		Use:   "naive",
		Short: "A naive implementation of 1brc",
		Run:   compute(naive),
	}

	computePcCmd = &cobra.Command{
		Use:   "chain",
		Short: "A producer-consumer implementation of 1brc",
		Run:   compute(chain),
	}

	computePcpCmd = &cobra.Command{
		Use:   "parallel",
		Short: "A parallel producer-consumer implementation of 1brc",
		Run:   compute(pcp),
	}

	computePcpStagedCmd = &cobra.Command{
		Use:   "staged",
		Short: "A parallel staged producer-consumer implementation of 1brc",
		Run:   compute(pcpStaged),
	}
)

func init() {
	rootCmd.PersistentFlags().
		Bool("p", false, "enable cpu profiling")
	rootCmd.PersistentFlags().
		String("profiler_output", "cpu.prof", "cpu profiler output file")

	generateCmd.Flags().
		StringP("output", "o", "measurements.csv", "output file")
	generateCmd.Flags().
		IntP("records", "r", 100, "number of records to generate")
	generateCmd.Flags().
		IntP("workers", "w", 1, "number of workers")
	generateCmd.Flags().
		IntP("size", "s", 1, "size of the chunk to generate")

	computeCmd.PersistentFlags().
		StringP("file", "f", "measurements.csv", "input file")
	computeCmd.PersistentFlags().
		IntP("iterations", "n", 1, "number of iterations to run the computation")
	computeCmd.PersistentFlags().
		IntP("buffer", "b", 1024, "buffer size for the buffered reader")

	readCmd.PersistentFlags().
		StringP("file", "f", "measurements.csv", "input file")
	readCmd.PersistentFlags().
		IntP("iterations", "n", 1, "number of iterations to run the computation")
	readCmd.PersistentFlags().
		IntP("buffer", "b", 1024, "buffer size for the buffered reader")

	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(computeCmd)
	rootCmd.AddCommand(readCmd)

	computeCmd.AddCommand(computeNaiveCmd)
	computeCmd.AddCommand(computePcCmd)
	computeCmd.AddCommand(computePcpCmd)
	computeCmd.AddCommand(computePcpStagedCmd)

	readCmd.AddCommand(readBufferCmd)
	readCmd.AddCommand(readBytesCmd)
	readCmd.AddCommand(readParallelCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Process failed with an error: [%s]\n", err)
		os.Exit(1)
	}
}

func printCommands(cmd *cobra.Command, _ []string) {
	fmt.Println("Available commands:")
	for _, command := range cmd.Commands() {
		fmt.Printf("  %s: %s\n", command.Name(), command.Short)
	}
}

func generate(cmd *cobra.Command, _ []string) {
	defer withProfiler(cmd)()

	config := parseGenerateConfig(cmd)

	fmt.Printf("Generating [%d] records with [%d] workers\n", config.records, config.workers)
	fmt.Printf("Output file: [%s]\n", config.output)

	generateMeasurements(config)
}

func read(f func(ComputeConfig)) func(cmd *cobra.Command, _ []string) {
	return func(cmd *cobra.Command, args []string) {
		defer withProfiler(cmd)()

		config := parseComputeConfig(cmd)

		summary := beauty.NewSummary()

		fmt.Printf("Reading data from file [%s]\n", config.file)
		for i := 0; i < config.iterations; i++ {
			start := time.Now()
			f(config)
			summary.Record(int(time.Since(start).Milliseconds()))
		}
		fmt.Printf("Reading data completed, summary [%s]\n", summary.Summary())
	}
}

func compute(f func(ComputeConfig)) func(cmd *cobra.Command, _ []string) {
	return func(cmd *cobra.Command, args []string) {
		defer withProfiler(cmd)()

		config := parseComputeConfig(cmd)

		summary := beauty.NewSummary()

		fmt.Printf("Processing data from file [%s]\n", config.file)
		for i := 0; i < config.iterations; i++ {
			start := time.Now()
			f(config)
			summary.Record(int(time.Since(start).Milliseconds()))
		}
		fmt.Printf("Processing data completed, summary [%s]\n", summary.Summary())
	}
}

func withProfiler(cmd *cobra.Command) func() {
	runProfiler := Must(cmd.Flags().GetBool("p"))
	profileFile := Must(cmd.Flags().GetString("profiler_output"))

	if !runProfiler {
		return func() {}
	}

	fmt.Printf("Starting CPU profiler\n")

	file, _ := os.Create(profileFile)
	if err := pprof.StartCPUProfile(file); err != nil {
		fmt.Printf("Failed to start the CPU profiler: [%s]\n", err)
		return nil
	}

	return func() {
		fmt.Printf("Stopping CPU profiler, output file: [%s]\n", profileFile)
		pprof.StopCPUProfile()
	}
}

func parseGenerateConfig(cmd *cobra.Command) GenerateConfig {
	output := Must(cmd.Flags().GetString("output"))
	records := Must(cmd.Flags().GetInt("records"))
	workers := Must(cmd.Flags().GetInt("workers"))
	chunkSize := Must(cmd.Flags().GetInt("size"))

	return GenerateConfig{
		output:       output,
		records:      records,
		workers:      workers,
		maxChunkSize: chunkSize,
	}
}

func parseComputeConfig(cmd *cobra.Command) ComputeConfig {
	file := Must(cmd.Flags().GetString("file"))
	iterations := Must(cmd.Flags().GetInt("iterations"))
	buffer := Must(cmd.Flags().GetInt("buffer"))

	return ComputeConfig{
		file:       file,
		iterations: iterations,
		bufferSize: buffer,
	}
}
