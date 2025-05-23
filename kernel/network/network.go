package network

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"ksql/config"
	"ksql/static"
	_ "ksql/topics"
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
	timeoutSec *int64
}

// Init - entry point for all ksql usage
// it initiates http connection with ksql-client
func Init(config config.Config) {
	client := http.Client{}

	if config.TimeoutSec != nil {
		client.Timeout = time.Duration(*config.TimeoutSec) * time.Second
	} else {
		client.Timeout = static.KsqlConnTimeout
	}

	Net = network{
		host:       config.Host,
		httpClient: &client,
		timeoutSec: config.TimeoutSec,
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

	req, err := http.NewRequestWithContext(
		ctx,
		method,
		n.host,
		bytes.NewReader([]byte(query)),
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
	defer resp.Body.Close()

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
