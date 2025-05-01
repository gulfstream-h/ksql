package network

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Network struct {
	host       string
	mu         sync.Mutex
	rps        atomic.Int32
	maxRps     int32
	httpClient *http.Client
	timeoutSec *int64
}

func New(
	host string,
	timeoutSec *int64) *Network {

	client := http.Client{}

	if timeoutSec != nil {
		client.Timeout = time.Duration(*timeoutSec) * time.Second
	}

	return &Network{
		host:       host,
		httpClient: &client,
		timeoutSec: timeoutSec,
	}
}
