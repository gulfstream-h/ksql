package network

import (
	"errors"
	"ksql/config"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	Net Network
)

type Network struct {
	host       string
	mu         sync.Mutex
	rps        atomic.Int32
	maxRps     int32
	httpClient *http.Client
	timeoutSec *int64
}

func InitNet(config config.Config) {
	client := http.Client{}

	if config.TimeoutSec != nil {
		client.Timeout = time.Duration(*config.TimeoutSec) * time.Second
	}

	Net = Network{
		host:       config.KsqlDbServer,
		httpClient: &client,
		maxRps:     int32(config.MaxConnTCP),
		timeoutSec: config.TimeoutSec,
	}
}

var (
	ErrRebalance = errors.New("too many requests for long polling streams. Rebalance in second...")
)
