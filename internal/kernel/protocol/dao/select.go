package dao

type Header struct {
	Header HeaderData `json:"header"`
}

type HeaderData struct {
	QueryID string `json:"queryId"`
	Schema  string `json:"schema"`
}

type Row struct {
	Row          Columns `json:"row"`
	FinalMessage string  `json:"finalMessage,omitempty"`
}

type Columns struct {
	Columns []any `json:"columns"`
}
