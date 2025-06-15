package main

import (
	"fmt"
	"ksql/config"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
)

func maine() {
	network.Init(config.Config{
		Host:       "http://localhost:8088",
		TimeoutSec: 30,
	})

	query := "CREATE STREAM user_events (user_id STRING,event_type STRING,created_at BIGINT) WITH (kafka_topic = 'user_events',  value_format = 'JSON',  partitions = 1);"
	query = "INSERT INTO user_events (user_id, event_type, created_at) VALUES (''u123'', ''login'', 1718385550);"

	d := protocol.GetRestDeserializer()

	fmt.Println(d.Deserialize(query))
}
