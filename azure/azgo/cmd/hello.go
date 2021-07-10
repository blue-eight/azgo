package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "hello [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE:  Hello,
	}
	rootCmd.AddCommand(mainCmd)
}

func Hello(cmd *cobra.Command, args []string) error {
	fmt.Printf("hello %s\n", args[0])
	return nil
}
