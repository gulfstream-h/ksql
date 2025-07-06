package cli

import (
	"context"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/migrations"
	"github.com/spf13/cobra"
	"log/slog"
)

// downCmd represents the up command
// that copies migration-file down command
// and executes it on remote ksql-server
var downCmd = &cobra.Command{
	Use:   "down [file_name]",
	Short: "Discard changes. Invokes down-migration in provided file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := config.New(dbURL, 30, false).Configure(context.Background())
		if err != nil {
			slog.Error("cannot initialize config", "error", err.Error())
			return
		}

		if err := migrations.New(dbURL, ".").Down(args[0]); err != nil {
			slog.Error("cannot down migration", "error", err.Error())
			return
		}

		slog.Info("down migration successfully executed", "filename", args[0])
	},
}

func init() {
	rootCmd.AddCommand(downCmd)
}
