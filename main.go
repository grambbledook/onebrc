package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"onebrc/beauty"
	"os"
	"runtime/pprof"
	"time"
)

type ExecutionConfig struct {
	Iterations int
}

var (
	rootCmd = &cobra.Command{
		Use: "obebrc",
		Run: PrintCommands,
	}

	generateCmd = &cobra.Command{
		Use:   "generate",
		Short: "Generate random measurements",
		Run:   Generate,
	}

	computeCmd = &cobra.Command{
		Use:   "compute",
		Short: "Process measurements",
		Run:   PrintCommands,
	}

	readCmd = &cobra.Command{
		Use:   "read",
		Short: "Read measurements file",
		Run:   PrintCommands,
	}

	readBufferCmd = &cobra.Command{
		Use:   "buffer",
		Short: "Read measurements file using buffered reader",
		Run:   Compute(CreateBufferedReaderTask),
	}

	readBytesCmd = &cobra.Command{
		Use:   "bytes",
		Short: "Read measurements file using buffered reader",
		Run:   Compute(CreateBufferedReaderBytesTask),
	}

	readParallelCmd = &cobra.Command{
		Use:   "parallel",
		Short: "Read measurements file using buffered reader",
		Run:   Compute(CreateParallelReaderTask),
	}

	computeNaiveCmd = &cobra.Command{
		Use:   "naive",
		Short: "A naive implementation of 1brc",
		Run:   Compute(CreateNaiveTask),
	}

	computePcCmd = &cobra.Command{
		Use:   "sequential",
		Short: "A producer-consumer implementation of 1brc",
		Run:   Compute(CreateProducerConsumerTask),
	}

	computePcpCmd = &cobra.Command{
		Use:   "parallel",
		Short: "A parallel producer-consumer implementation of 1brc",
		Run:   Compute(CreateParallelProducerConsumerTask),
	}

	computePcpStagedCmd = &cobra.Command{
		Use:   "staged",
		Short: "A parallel staged producer-consumer implementation of 1brc",
		Run:   Compute(CreateParallelStagedProducerConsumerTask),
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
		IntP("records", "r", 100, "number of records to Generate")
	generateCmd.Flags().
		IntP("workers", "w", 1, "number of workers")
	generateCmd.Flags().
		IntP("size", "s", 1, "size of the chunk to Generate")

	computeCmd.PersistentFlags().
		StringP("file", "f", "measurements.csv", "input file")
	computeCmd.PersistentFlags().
		BoolP("integers", "i", false, "use integer numbers in computations")
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

func PrintCommands(cmd *cobra.Command, _ []string) {
	fmt.Println("Available commands:")
	for _, command := range cmd.Commands() {
		fmt.Printf("  %s: %s\n", command.Name(), command.Short)
	}
}

func Generate(cmd *cobra.Command, _ []string) {
	defer WithProfiler(cmd)()

	task := CreateGenerateTask(cmd)

	fmt.Printf("Generating [%d] records with [%d] workers\n", task.records, task.workers)
	fmt.Printf("Output file: [%s]\n", task.output)

	task.Execute()
}

func Compute(createTask func(cmd *cobra.Command) Task) func(cmd *cobra.Command, _ []string) {
	return func(cmd *cobra.Command, args []string) {
		defer WithProfiler(cmd)()

		executionConfig := CreateExecutionConfig(cmd)
		task := createTask(cmd)

		summary := beauty.NewSummary()

		fmt.Printf("Executing task [%s]\n", task.Name())
		fmt.Printf(" Processing data from the file: [%s]\n", task.File())

		for i := 0; i < executionConfig.Iterations; i++ {
			start := time.Now()

			task.Execute()

			summary.Record(int(time.Since(start).Milliseconds()))
		}

		fmt.Printf(" Task completed, summary [%s]\n", summary.Summary())
	}
}

func WithProfiler(cmd *cobra.Command) func() {
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

func CreateGenerateTask(cmd *cobra.Command) GenerateTask {
	output := Must(cmd.Flags().GetString("output"))
	records := Must(cmd.Flags().GetInt("records"))
	workers := Must(cmd.Flags().GetInt("workers"))
	chunkSize := Must(cmd.Flags().GetInt("size"))

	return GenerateTask{
		output:       output,
		records:      records,
		workers:      workers,
		maxChunkSize: chunkSize,
	}
}

func CreateBufferedReaderTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))

	return BufferedReaderTask{
		file:       file,
		bufferSize: buffer,
	}
}

func CreateBufferedReaderBytesTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))

	return BufferedReaderBytesTask{
		file:       file,
		bufferSize: buffer,
	}
}

func CreateParallelReaderTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))

	return ParallelBufferedReaderTask{
		file:       file,
		bufferSize: buffer,
	}
}

func CreateExecutionConfig(cmd *cobra.Command) ExecutionConfig {
	return ExecutionConfig{
		Iterations: Must(cmd.Flags().GetInt("iterations")),
	}
}

func CreateNaiveTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))
	useInts := Must(cmd.Flags().GetBool("integers"))

	if useInts {
		return NaiveComputeTask[int]{
			file:       file,
			bufferSize: buffer,
			lineParser: ParseInt,
		}
	}

	return NaiveComputeTask[float32]{
		file:       file,
		bufferSize: buffer,
		lineParser: ParseFloat,
	}
}

func CreateProducerConsumerTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))
	useInts := Must(cmd.Flags().GetBool("integers"))

	if useInts {
		return ProducerConsumerTask[int]{
			file:       file,
			bufferSize: buffer,
			lineParser: ParseInt,
		}
	}

	return ProducerConsumerTask[float32]{
		file:       file,
		bufferSize: buffer,
		lineParser: ParseFloat,
	}
}

func CreateParallelProducerConsumerTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))
	useInts := Must(cmd.Flags().GetBool("integers"))

	if useInts {
		return ParallelProducerConsumerTask[int]{
			file:       file,
			bufferSize: buffer,
			lineParser: ParseInt,
		}
	}

	return ParallelProducerConsumerTask[float32]{
		file:       file,
		bufferSize: buffer,
		lineParser: ParseFloat,
	}
}

func CreateParallelStagedProducerConsumerTask(cmd *cobra.Command) Task {
	file := Must(cmd.Flags().GetString("file"))
	buffer := Must(cmd.Flags().GetInt("buffer"))
	useInts := Must(cmd.Flags().GetBool("integers"))

	if useInts {
		return ParallelStagedProducerConsumerTask[int]{
			file:       file,
			bufferSize: buffer,
			lineParser: ParseInt,
		}
	}

	return ParallelStagedProducerConsumerTask[float32]{
		file:       file,
		bufferSize: buffer,
		lineParser: ParseFloat,
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Process failed with an error: [%s]\n", err)
		os.Exit(1)
	}
}
