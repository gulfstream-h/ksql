package config

type Config struct {
	KsqlDbServer string
	MaxConnTCP   int64
	TimeoutSec   int64
}
