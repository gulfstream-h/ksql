package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"io"
	"ksql/static"
	"net/http"
	"time"
)

var (
	// Net - is a global ksql http proxy
	// can be called in topics, streams and tables packages
	Net network
)

type network struct {
	host       string
	httpClient *http.Client
	timeoutSec int64
}

// Init - entry point for all ksql usage
// it initiates http connection with ksql-client
func Init(host string, timeout time.Duration) {
	client := http.Client{
		Timeout: timeout,
	}

	Net = network{
		host:       host,
		httpClient: &client,
	}
}

// Poller - provides different strategies of kafka http processing
// Like for short polling requests it handles only one full stream of data
// For Long-Polling requests it provides row-by-row parsing with open connection
// Mostly all ksql requests are short polling, however EMIT_CHANGES requests are
// long polling
type Poller interface {
	Process(io.ReadCloser) <-chan []byte
}

// Perform - is common for all net request logic,
// responsible to initiate and properly close connection
// As all ksql queries shares the same http params,
// current module sets it as default for code-reduce purpose
func (n *network) Perform(
	ctx context.Context,
	method string,
	query string,
	pollingAlgo Poller) (<-chan []byte, error) {

	q, _ := jsoniter.Marshal(struct {
		KSQL string `json:"ksql"`
	}{
		KSQL: query,
	})

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		n.host+"/ksql",
		bytes.NewReader(q),
	)
	if err != nil {
		return nil, fmt.Errorf("error while formating req: %w", err)
	}

	req.Header.Set(
		static.ContentType,
		static.HeaderKSQL,
	)

	var (
		resp *http.Response
	)

	if resp, err = n.httpClient.Do(req); err != nil {
		return nil, fmt.Errorf("error while performing req: %w", err)
	}

	return pollingAlgo.Process(resp.Body), nil
}

func (n *network) PerformSelect(
	ctx context.Context,
	method string,
	query string,
	pollingAlgo Poller) (<-chan []byte, error) {

	q, _ := jsoniter.Marshal(struct {
		KSQL string `json:"ksql"`
	}{
		KSQL: query,
	})

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		n.host+"/query",
		bytes.NewReader(q),
	)
	if err != nil {
		return nil, fmt.Errorf("error while formating req: %w", err)
	}
	req.Header.Set(
		static.ContentType,
		static.HeaderKSQL,
	)

	var (
		resp *http.Response
	)

	cli := http.DefaultClient

	if resp, err = cli.Do(req); err != nil {
		return nil, fmt.Errorf("error while performing req: %w", err)
	}

	return pollingAlgo.Process(resp.Body), nil
}

type (
	ShortPolling struct{}
)

// Process - performs fast and ordinary http request
// it can be used for show, describe, drop, create, insert and most of select queries
func (sp ShortPolling) Process(
	payload io.ReadCloser,
) <-chan []byte {

	ch := make(chan []byte, 1)

	go func() {
		defer payload.Close()
		defer close(ch)

		buffer, err := io.ReadAll(payload)
		if err != nil {
			return
		}
		ch <- buffer
	}()

	return ch
}

type (
	LongPolling struct{}
)

// Process - performs long-living requests. Mostly SELECT with EMIT CHANGES.
// Channel is closed only on receiving EOF from KSQL-Client
func (lp LongPolling) Process(
	payload io.ReadCloser) <-chan []byte {

	ch := make(chan []byte)

	go func() {
		scanner := bufio.NewScanner(payload)

		for scanner.Scan() {
			line := scanner.Text()

			if len(line) == 0 {
				continue
			}

			ch <- []byte(line)
		}

		if err := scanner.Err(); err != nil {
			return
		}
	}()

	return ch
}
