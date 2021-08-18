package cmd

import (
	"github.com/blue-eight/azgo/azgo/table"
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
	return table.ListTables()
}
