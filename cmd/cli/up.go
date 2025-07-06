package cli

import (
	"context"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/migrations"
	"github.com/spf13/cobra"
	"log/slog"
)

// upCmd represents the up command
// that copies migration-file up command
// and executes it on remote ksql-server
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
