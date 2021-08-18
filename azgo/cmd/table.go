package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/blue-eight/azgo/azgo/table"
	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "table",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-list",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.ListTables()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-create [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.CreateTable(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-delete [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.DeleteTable(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "insert-kv [table] [key] [value]",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.InsertKeyValue(args[0], args[1], args[2])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "upsert-kv [table] [key] [value]",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.UpsertKeyValue(args[0], args[1], args[2])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "insert [table] [json]",
		Short: "...",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.InsertJSON(args[0], []byte(args[1]))
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "insert-stdin [table]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.InsertStdin(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "get [table] [partition-key] [row-key]",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := table.Get(args[0], args[1], args[2])
			if err != nil {
				return err
			}
			b, err := json.Marshal(resp)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
			return nil
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "delete [table] [partition-key] [row-key]",
		Short: "...",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := table.Delete(args[0], args[1], args[2])
			if err != nil {
				return err
			}
			b, err := json.Marshal(resp)
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
			return nil
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "query [table] [query]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filter := ""
			if len(args) == 2 {
				filter = args[1]
			}
			return table.Query(args[0], filter)
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "query-delete [table] [query]",
		Short: "...",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return table.QueryDelete(args[0], args[1])
		},
	})

	rootCmd.AddCommand(mainCmd)

}
