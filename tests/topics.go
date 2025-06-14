package main

import (
	"context"
	"fmt"
	"ksql/config"
	"ksql/kernel/network"
	"ksql/kinds"
	"ksql/shared"
	"ksql/streams"
)

func main() {
	network.Init(config.Config{
		Host:       "http://localhost:8088",
		TimeoutSec: 30,
	})

	//listStreams()
	//describeStream()
	//getStream()

	//dropStream()
	//createStream()

	//insert()

	get()
}

func listStreams() {
	streams, err := streams.ListStreams(context.TODO())
	if err != nil {
		panic(err)
	}
	fmt.Println(streams)
}

func describeStream() {
	desc, err := streams.Describe(context.TODO(), "NEW_STREAM")
	if err != nil {
		panic(err)
	}
	fmt.Println(desc)
}

func getStream() {
	type MyStream struct {
		ID   int    `ksql:"id"`
		Name string `ksql:"name"`
	}

	stream, err := streams.GetStream[MyStream](context.TODO(), "MY_STREAM")
	if err != nil {
		panic(err)
	}

	fmt.Println(stream)
}

func dropStream() {
	if err := streams.Drop(context.TODO(), "NEW_STREAM"); err != nil {
		panic(err)
	}
}

func createStream() {
	type NewStream struct {
		Amount     int    `ksql:"amount"`
		ClientHash string `ksql:"client_hash"`
	}

	topic := "example_topic"
	partitions := uint8(3)

	newStream, err := streams.CreateStream[NewStream](
		context.TODO(),
		"NEW_STREAM", shared.StreamSettings{
			SourceTopic: &topic,
			Partitions:  &partitions,
			Format:      kinds.JSON,
		})
	if err != nil {
		panic(err)
	}

	fmt.Println(newStream)
}

func insert() {
	type NewStream struct {
		Amount     int    `ksql:"amount"`
		ClientHash string `ksql:"client_hash"`
	}

	newStream, err := streams.GetStream[NewStream](context.TODO(), "NEW_STREAM")
	if err != nil {
		panic(err)
	}

	if err = newStream.Insert(context.TODO(), map[string]any{
		"amount":      123,
		"client_hash": "dwdw",
	}); err != nil {
		panic(err)
	}

}

func get() {
	type NewStream struct {
		Amount     int    `ksql:"amount"`
		ClientHash string `ksql:"client_hash"`
	}

	newStream, err := streams.GetStream[NewStream](
		context.TODO(),
		"NEW_STREAM")
	if err != nil {
		panic(err)
	}

	val, err := newStream.SelectOnce(context.TODO())
	if err != nil {
		panic(err)
	}

	fmt.Println(val)
}
