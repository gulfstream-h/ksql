package cmd

import (
	"github.com/spf13/cobra"
	"ksql/config"
	"ksql/kernel/network"
	"ksql/migrations"
	"log/slog"
)

// upCmd represents the up command
var upCmd = &cobra.Command{
	Use:   "up [file_name]",
	Short: "Apply changes. Invokes up-migration in provided file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		network.Init(config.Config{
			Host:       dbURL,
			TimeoutSec: 30,
		})

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
