package main

import (
	"context"
	"fmt"
	"ksql/config"
	"ksql/streams"
)

func main() {
	type MapTest struct {
		ID   int64  `ksql:"ID"`
		Hash []byte `ksql:"HASH"`
	}

	//topic := "hash_test-topic"
	//part := uint8(1)

	if err := config.New("http://localhost:8088", 30, false).Configure(context.Background()); err != nil {
		panic(err)
	}

	//streams.Drop(context.Background(), "hash_test")
	//
	//_, err := streams.CreateStream[MapTest](context.Background(), "hash_test", shared.StreamSettings{
	//	Name:        "map",
	//	SourceTopic: &topic,
	//	Partitions:  &part,
	//	Schema:      nil,
	//	Format:      kinds.JSON,
	//	DeleteFunc:  nil,
	//})
	//if err != nil {
	//	panic(err)
	//}

	stream, err := streams.GetStream[MapTest](context.Background(), "hash_test")
	if err != nil {
		panic(err)
	}

	//var base []byte
	//base64.StdEncoding.Encode([]byte("test_hash"), base)

	//if err = stream.Insert(context.Background(), map[string]any{
	//	"ID":   1,
	//	"HASH": base,
	//}); err != nil {
	//	panic(err)
	//}

	c, err := stream.SelectOnce(context.Background())

	if err != nil {
		panic(err)
	}

	fmt.Println("Received data:", c)

}
