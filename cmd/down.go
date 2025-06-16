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

// downCmd represents the down command
var downCmd = &cobra.Command{
	Use:   "down [file_name]",
	Short: "Discard changes. Invokes down-migration in provided file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		network.Init(config.Config{
			Host:       dbURL,
			TimeoutSec: 30,
		})

		if err := migrations.New(dbURL, ".").Down(args[0]); err != nil {
			log.Fatalf("cannot down migration " + err.Error())
		}
	},
}

func init() {
	rootCmd.AddCommand(downCmd)
}
