package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var (
	cmd = &cobra.Command{
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
			output, _ := cmd.Flags().GetString("output")
			records, _ := cmd.Flags().GetInt("records")
			workers, _ := cmd.Flags().GetInt("workers")
			chunkSize, _ := cmd.Flags().GetInt("size")

			fmt.Printf("Generating [%d] records\n", records)
			fmt.Printf("Output file: [%s]\n", output)

			generate(
				GenerateConfig{output, records, workers, chunkSize},
			)
		},
	}
)

func init() {
	generateCmd.Flags().
		StringP("output", "o", "measurements.csv", "output file")
	generateCmd.Flags().
		IntP("records", "r", 100, "number of records to generate")
	generateCmd.Flags().
		IntP("workers", "w", 1, "number of workers")
	generateCmd.Flags().
		IntP("size", "s", 1, "size of the chunk to generate")

	cmd.AddCommand(generateCmd)
}

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Printf("Process failed with an error: [%s]\n", err)
		os.Exit(1)
	}
}
