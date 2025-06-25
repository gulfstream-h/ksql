package consts

import "time"

const (
	KSQL            = "ksql"
	KsqlConnTimeout = 30 * time.Second
)

const (
	SUCCESS = "SUCCESS"
)

const (
	ContentType = "Content-Type"
	HeaderKSQL  = "application/vnd.ksql.v1+json"
)
