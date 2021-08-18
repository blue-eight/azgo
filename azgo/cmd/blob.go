package cmd

import (
	"fmt"

	"github.com/blue-eight/azgo/azgo/blob"
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
		Use:   "container-list",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.ListContainers()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "container-create [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.CreateContainer(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "container-delete [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.DeleteContainer(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "insert-kv [container] [key] [value]",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.InsertKeyValue(args[0], args[1], args[2])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "get [container] [key]",
		Short: "...",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			value, err := blob.Get(args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", value)
			return nil
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "delete [container] [key]",
		Short: "...",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := blob.Delete(args[0], args[1])
			if err != nil {
				return err
			}
			return nil
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "list [container]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.List(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return blob.Test()
		},
	})

	rootCmd.AddCommand(mainCmd)

}
