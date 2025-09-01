package main

import (
	"github.com/joho/godotenv"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
)

var dbURL string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use: "ksql",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// if ksql_url is not provided in command via --db_url tag, seeking in .env file
		if !cmd.Flags().Changed("db_url") {
			if err := godotenv.Load(); err != nil {
				slog.Error("cannot load .env file", "error", err)
			}

			if env := os.Getenv("KSQL_DB_URL"); env != "" {
				dbURL = env
			}
		}
	},
	Short: "Migration tool to KSQL-server",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.
		PersistentFlags().
		StringVar(&dbURL, "db_url", "", "database URL")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
