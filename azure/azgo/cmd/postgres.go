package cmd

import (
	"github.com/blue-eight/private/azure/azgo/postgres"
	"github.com/spf13/cobra"
)

func init() {
	var mainCmd = &cobra.Command{
		Use:   "postgres",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-list",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.ListTables()
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-create [name] [?type]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			valueType := ""
			if len(args) == 2 {
				if args[1] == "jsonb" || args[1] == "text" || args[1] == "json" {
					valueType = args[1]
				}
			}
			return postgres.CreateTable(args[0], valueType)
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "table-delete [name]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.DeleteTable(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "insert-stdin [table]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: consider making batchSize configurable here
			return postgres.InsertStdinBulk(args[0], 100)
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "query [query]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.QueryString(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "query-json [query]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.QueryJSON(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "query-kv [query]",
		Short: "...",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.QueryKeyValue(args[0])
		},
	})

	mainCmd.AddCommand(&cobra.Command{
		Use:   "test",
		Short: "...",
		RunE: func(cmd *cobra.Command, args []string) error {
			return postgres.Test()
		},
	})

	rootCmd.AddCommand(mainCmd)

}
