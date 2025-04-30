package nets

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
)

var (
	ErrCannotWriteData = errors.New("cannot write info by net")
	ErrCannotReadData  = errors.New("cannot read data by net")
)

func formatCurl(host string, contentLen int, request string) string {
	return fmt.Sprintf("POST /ksql HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Content-Type: application/vnd.ksql.v1+json; charset=utf-8\r\n"+
		"Content-Length: %d\r\n"+
		"Connection: close\r\n\r\n%s", host, contentLen, request)
}

func processRequest(ctx context.Context, conn net.Conn, request []byte) ([]byte, error) {
	if _, err := bufio.NewWriter(conn).Write(request); err != nil {
		return nil, errors.Join(ErrCannotWriteData, err)
	}

	var (
		response []byte
	)

	if _, err := bufio.NewReader(conn).Read(response); err != nil {
		return nil, errors.Join(ErrCannotReadData, err)
	}

	return response, nil
}

func Perform(ctx context.Context, conn net.Conn, host string, contentLen int, request string) ([]byte, error) {
	return processRequest(ctx, conn, []byte(formatCurl(host, contentLen, request)))
}
