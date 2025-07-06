package config

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/errors"
	"github.com/gulfstream-h/ksql/internal/kernel/network"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/static"
	"sync"
	"time"
)

var (
	once sync.Once
)

// config - is user defined structure
// aimed to set exact settings for client
type config struct {
	Host           string // remote address of ksql server
	TimeoutSec     int64  // request timeout in seconds
	reflectionFlag bool
	shared.Linter  // enables query lintering with reflection
}

// New returns config file for ksql-connection
// establishment. It's also configures reflection mode
// in which library is executed
func New(
	host string,
	timeoutSec int64,
	reflectionFlag bool) shared.Config {

	var cfg = config{
		Host:       host,
		TimeoutSec: timeoutSec,
	}

	static.ReflectionFlag = reflectionFlag

	if reflectionFlag {
		cfg.Linter = _ReflectionMode{}
	} else {
		cfg.Linter = _NoReflectionMode{}
	}

	return &cfg
}

// Configure - applying method for structures
// it initialize network connection. Entry-point for library
// net operations
func (cfg *config) Configure(ctx context.Context) (err error) {
	once.Do(func() {
		if cfg.Host == "" {
			err = errors.ErrMissingHost
			return
		}

		if cfg.TimeoutSec <= 0 {
			err = errors.ErrTimeoutIsZeroOrNegative
			return
		}

		network.Init(cfg.Host, time.Duration(cfg.TimeoutSec)*time.Second)

		if err = cfg.Linter.InitLinter(ctx); err != nil {
			err = fmt.Errorf("cannot run lintering: %w", err)
			return
		}
	})

	return nil
}
