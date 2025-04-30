package conn

import (
	"context"
	"errors"
	"fmt"
	"ksql/kernel/network"
	"net"
	"sync"
	"time"
)

type kafkaConnection struct {
	mu   sync.RWMutex
	conn net.Conn
}

func NewConnection(host string) Connection {
	conn, _ := net.Dial("tcp", host)
	return &kafkaConnection{conn: conn}
}

type Connection interface {
	Ping(context.Context) error
	Close() error
}

var (
	ErrCannotDialToKafka   = errors.New("cannot dial to kafka client")
	ErrConnectionNotClosed = errors.New("kafkaConnection was not closed properly")
)

func (c *kafkaConnection) Ping(ctx context.Context) (err error) {
	c.mu.RLock()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}

	c.conn, err = net.DialTimeout(
		"tcp",
		c.conn.RemoteAddr().String(),
		deadline.Sub(time.Now()))
	if err != nil {
		return errors.Join(ErrCannotDialToKafka, err)
	}

	return
}

func (c *kafkaConnection) Close() error {
	err := c.conn.Close()
	if err != nil {
		return errors.Join(ErrConnectionNotClosed, err)
	}

	return nil
}

const (
	ShowStreamsCommand = "SHOW STREAMS;"
)

func (c kafkaConnection) ListStreams(ctx context.Context) {
	showStreamsRequest := formatCommand(ShowStreamsCommand)

	response, err := network.Perform(
		ctx,
		c.conn,
		c.conn.RemoteAddr().String(),
		len(showStreamsRequest),
		showStreamsRequest)
	if err != nil {
		return
	}

	fmt.Println(string(response))
}

func formatCommand(command string) string {
	return fmt.Sprintf(`{"ksql": "%s"}`, command)
}

//curl -X "POST" "http://localhost:8088/ksql" \
//    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
//    -d '{
//          "ksql": "CREATE STREAM my_stream (id INT, name STRING) WITH (kafka_topic='\''example_topic'\'', value_format='\''JSON'\'');"
//        }'

//curl -X "POST" "http://localhost:8088/ksql" \
//    -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
//    -d '{
//          "ksql": "SHOW STREAMS;"
//        }'
