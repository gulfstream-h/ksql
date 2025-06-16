package main

import (
	"context"
	"fmt"
	"ksql/config"
	"ksql/kernel/network"
	"ksql/kinds"
	"ksql/shared"
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
	"time"
)

func mainr() {
	network.Init(config.Config{
		Host:       "http://localhost:8088",
		TimeoutSec: 30,
	})

	//showTopics()

	//////////////

	//listStreams()
	//describeStream()
	//getStream()

	//dropStream()
	//createStream()

	//insert()

	//get()
	//getWithEmit()

	//////////////

	//showTables()
	//describeTable()
	//dropTable()
	//getTable()

	//dropTable()
	//createTable()
	//getT()
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
	if err := streams.Drop(context.TODO(), "SEEKER_STREAM"); err != nil {
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

	go func() {
		t := time.NewTicker(5 * time.Second)
		i := 0
		for {

			select {
			case <-t.C:
				if err = newStream.Insert(context.TODO(), map[string]any{
					"amount":      i,
					"client_hash": "dwdw",
				}); err != nil {
					panic(err)
				}
				i++
			}
		}

	}()

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

func getWithEmit() {
	type NewStream struct {
		Amount     int    `ksql:"amount"`
		ClientHash string `ksql:"client_hash"`
	}

	//insert()

	newStream, err := streams.GetStream[NewStream](
		context.TODO(),
		"NEW_STREAM")
	if err != nil {
		panic(err)
	}

	//Query Completed"

	val, err := newStream.SelectWithEmit(context.TODO())
	if err != nil {
		panic(err)
	}

	for v := range val {
		fmt.Println(v)
	}
}

func showTopics() {
	topic, err := topics.ListTopics(context.TODO())
	if err != nil {
		panic(err)
	}

	fmt.Println(topic)
}

func showTables() {
	tabl, err := tables.ListTables(context.TODO())
	if err != nil {
		panic(err)
	}
	fmt.Println(tabl)
}

func describeTable() {
	desc, err := tables.Describe(context.TODO(), "QUERYABLE_NEUE_TABLE")
	if err != nil {
		panic(err)
	}

	fmt.Println(desc)
}

func dropTable() {
	if err := tables.Drop(context.TODO(), "QUERYABLE_SEEKER_TABLE"); err != nil {
		panic(err)
	}

	if err := tables.Drop(context.TODO(), "SEEKER_TABLE"); err != nil {
		panic(err)
	}
}

func getTable() {
	type MyTable struct {
		ID   int    `ksql:"id"`
		Name string `ksql:"name"`
	}

	tabl, err := tables.GetTable[MyTable](context.TODO(), "MY_TABLE")
	if err != nil {
		panic(err)
	}

	fmt.Println(tabl)
}

func createTable() {
	type NewTable struct {
		Amount     int    `ksql:"amount"`
		ClientHash string `ksql:"client_hash, PRIMARY KEY"`
	}

	topic := "example_topic"

	table, err := tables.CreateTable[NewTable](context.TODO(), "NEUE_TABLE", shared.TableSettings{
		SourceTopic: &topic,
		Format:      kinds.JSON,
	})
	if err != nil {
		panic(err)
	}

	println(table)
}

func getT() {
	type NeuTable struct {
		Amount     int    `ksql:"AMOUNT"`
		ClientHash string `ksql:"CLIENT_HASH"`
	}

	tabl, err := tables.GetTable[NeuTable](context.TODO(), "NEUE_TABLE")
	if err != nil {
		panic(err)
	}

	c, err := tabl.SelectWithEmit(context.TODO())
	if err != nil {
		panic(err)
	}

	for r := range c {
		fmt.Println("response")
		fmt.Println(r)
	}
}
