package main

import (
	"ksql/config"
	"ksql/kernel/network"
	"ksql/migrations"
)

func main() {
	network.Init(config.Config{
		Host:       "http://localhost:8088",
		TimeoutSec: 30,
	})

	m := migrations.New("http://localhost:8088", "../migrations")
	if err := m.Up("1749923469_my_mig3.sql"); err != nil {
		panic(err)
	}
}
