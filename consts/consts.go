package consts

const (
	KSQL = "ksql" // struct field tag for custom structs
)

const (
	KsqlRoute  = "/ksql"  // used for http mode
	QueryRoute = "/query" // used for websocket mode
)

const (
	SUCCESS = "SUCCESS" // ksql responses with such literal status code
)

const (
	Queryable = "QUERYABLE" // tables prefix for selecting purpose
)

const (
	ContentType = "Content-Type"                 // http Header-Name
	HeaderKSQL  = "application/vnd.ksql.v1+json" // ksql Header
)
