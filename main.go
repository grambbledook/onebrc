package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

func main() {
	cmd := &cobra.Command{
		Use: "obebrc",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Available commands:")
			for _, command := range cmd.Commands() {
				fmt.Printf("  %s: %s\n", command.Name(), command.Short)
			}
		},
	}

	generate := &cobra.Command{
		Use:   "generate",
		Short: "Generate random measurements",
		Run: func(cmd *cobra.Command, args []string) {
			output, _ := cmd.Flags().GetString("output")
			records, _ := cmd.Flags().GetInt("records")
			workers, _ := cmd.Flags().GetInt("workers")
			chunkSize, _ := cmd.Flags().GetInt("size")

			fmt.Printf("Generating %d records\n ", records)
			fmt.Printf("Output file: %s\n ", output)

			generate(output, records, workers, chunkSize)
		},
	}

	generate.PersistentFlags().
		StringP("output", "o", "measurements.csv", "output file")
	generate.PersistentFlags().
		IntP("records", "r", 100, "number of records to generate")
	generate.PersistentFlags().
		IntP("workers", "w", 1, "number of workers")
	generate.PersistentFlags().
		IntP("size", "s", 1, "size of the chunk to generate")

	cmd.AddCommand(generate)

	if err := cmd.Execute(); err != nil {
		fmt.Printf("Process failed with an error: %s\n", err)
		os.Exit(1)
	}
}
