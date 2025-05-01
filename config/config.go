package config

import (
	"context"
)

type Config struct {
	KsqlDbServer string
	TimeoutSec   int64
}

func Init(cfg Config) {
	// Initialize the kernel with the provided configuration
}

func RegisterPool(ctx context.Context) {
	// Register the connection pool
}
