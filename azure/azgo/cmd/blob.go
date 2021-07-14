package cmd

import (
	"github.com/blue-eight/private/azure/azgo/table/blob"
	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "blob",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.Test()
		},
	})

	rootCmd.AddCommand(mainCmd)

}
