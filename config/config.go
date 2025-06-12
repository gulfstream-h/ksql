package config

// Config - is user defined structure
// aimed to set exact settings for client
type Config struct {
	Host       string // remote address of ksql server
	TimeoutSec int64  // request timeout in seconds
}
