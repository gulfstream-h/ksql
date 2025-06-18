package cmd

import (
	"context"
	"github.com/spf13/cobra"
	"ksql/config"
	"ksql/migrations"
	"log/slog"
)

// upCmd represents the up command
var upCmd = &cobra.Command{
	Use:   "up [file_name]",
	Short: "Apply changes. Invokes up-migration in provided file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := config.New(dbURL, 30, false).Configure(context.Background())
		if err != nil {
			slog.Error("cannot initialize config", "error", err.Error())
			return
		}

		if err := migrations.New(dbURL, ".").Up(args[0]); err != nil {
			slog.Error("cannot up migration", "error", err.Error())
			return
		}

		slog.Info("migration was successfully executed", "filename", args[0])
	},
}

func init() {
	rootCmd.AddCommand(upCmd)
}
