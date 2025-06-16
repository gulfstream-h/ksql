/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
	"ksql/config"
	"ksql/kernel/network"
	"ksql/migrations"
	"log"
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
			log.Fatalf("cannot up migration %w", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(upCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// upCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// upCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
