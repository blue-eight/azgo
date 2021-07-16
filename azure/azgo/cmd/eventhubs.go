package cmd

import (
	"github.com/blue-eight/private/azure/azgo/eventhubs"
	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "eventhubs",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "send [message]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return eventhubs.Send(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "send-stdin",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return eventhubs.SendStdin()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "receive",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return eventhubs.Receive()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return eventhubs.Test()
		},
	})

	rootCmd.AddCommand(mainCmd)

}
