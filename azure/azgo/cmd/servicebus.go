package cmd

import (
	"fmt"

	"github.com/blue-eight/private/azure/azgo/servicebus"
	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "servicebus",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "queue-list",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return servicebus.ListQueues()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "queue-create [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return servicebus.CreateQueue(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "queue-delete [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return servicebus.DeleteQueue(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "send [queue] [value]",
		Short: "...",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return servicebus.Send(args[0], args[1])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "receive [queue]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			val, err := servicebus.Receive(args[0])
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", val)
			return nil
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return servicebus.Test()
		},
	})

	rootCmd.AddCommand(mainCmd)

}
