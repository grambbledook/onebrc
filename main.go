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
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Available commands:")
			for _, command := range cmd.Commands() {
				fmt.Printf("  %s: %s\n", command.Name(), command.Short)
			}
		},
	}

	generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate random measurements",
		Run: func(cmd *cobra.Command, args []string) {
			startProfiler(cmd)
			defer stopProfiler(cmd)

			output, _ := cmd.Flags().GetString("output")
			records, _ := cmd.Flags().GetInt("records")
			workers, _ := cmd.Flags().GetInt("workers")
			chunkSize, _ := cmd.Flags().GetInt("size")

			fmt.Printf("Generating [%d] records\n", records)
			fmt.Printf("Output file: [%s]\n", output)

			generate(GenerateConfig{output, records, workers, chunkSize})
		},
	}

	computeCmd = &cobra.Command{
		Use:   "compute",
		Short: "Process measurements",
		Run:   compute(naive),
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

	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(computeCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Process failed with an error: [%s]\n", err)
		os.Exit(1)
	}
}

func compute(f func(string)) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		startProfiler(cmd)
		defer stopProfiler(cmd)

		file, _ := cmd.Flags().GetString("file")
		iterations, _ := cmd.Flags().GetInt("iterations")

		summary := beauty.NewSummary()

		fmt.Printf("Processing data from file [%s]\n", file)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			f(file)
			summary.Record(int(time.Since(start).Milliseconds()))
		}
		fmt.Printf("Processing data completed, summary [%s]\n", summary.Summary())
	}
}

func startProfiler(cmd *cobra.Command) {
	runProfiler, _ := cmd.Flags().GetBool("p")
	profileFile, _ := cmd.Flags().GetString("profiler_output")

	if runProfiler {
		fmt.Printf("Starting CPU profiler\n")

		file, _ := os.Create(profileFile)
		if err := pprof.StartCPUProfile(file); err != nil {
			fmt.Printf("Failed to start the CPU profiler: [%s]\n", err)
			return
		}
	}
}
func stopProfiler(cmd *cobra.Command) {
	runProfiler, _ := cmd.Flags().GetBool("p")
	profileFile, _ := cmd.Flags().GetString("profiler_output")

	if runProfiler {
		fmt.Printf("Stopping CPU profiler, output file: [%s]\n", profileFile)
		pprof.StopCPUProfile()
	}
}
