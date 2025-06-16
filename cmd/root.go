/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

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
		if !cmd.Flags().Changed("db_url") {
			if err := godotenv.Load(); err != nil {
				slog.Error("cannot load .env file", "error", err)
			}

			if env := os.Getenv("KSQL_DB_URL"); env != "" {
				dbURL = env
			}
		}
	},
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
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
