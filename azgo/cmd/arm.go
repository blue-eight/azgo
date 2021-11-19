package cmd

import (
	"github.com/blue-eight/azgo/azgo/arm"

	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "arm",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "deploy [subscriptionid] [resourcegroup] [location]",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return arm.DeployTemplateGroup(args[0], args[1], args[2])
		},
	})

	rootCmd.AddCommand(mainCmd)
}
