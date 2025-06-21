package config

import (
	"context"
	"fmt"
	"ksql/kernel/network"
	"ksql/shared"
	"ksql/static"
	"time"
)

// config - is user defined structure
// aimed to set exact settings for client
type config struct {
	Host          string // remote address of ksql server
	TimeoutSec    int64  // request timeout in seconds
	shared.Linter        // enables query lintering with reflection
}

func New(
	host string,
	timeoutSec int64,
	reflectionFlag bool) shared.Config {

	var cfg = config{
		Host:       host,
		TimeoutSec: timeoutSec,
	}

	if reflectionFlag {
		cfg.Linter = _NoReflectionMode{}
	} else {
		cfg.Linter = _ReflectionMode{}
	}

	return &cfg
}

func (cfg *config) Configure(ctx context.Context) error {
	if cfg.Host == "" {
		return static.ErrMissingHost
	}

	if cfg.TimeoutSec <= 0 {
		return static.ErrTimeoutIsZeroOrNegative
	}

	network.Init(cfg.Host, time.Duration(cfg.TimeoutSec)*time.Second)

	if err := cfg.Linter.InitLinter(ctx); err != nil {
		return fmt.Errorf("cannot run lintering: %w", err)
	}

	return nil
}
