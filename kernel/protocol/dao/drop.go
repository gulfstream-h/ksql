package dao

//curl -X POST \
//-H "Content-Type: application/vnd.ksql.v1+json" \
//-d '{
//  "ksql": "DROP STREAM TESTIFY;"
//}' \
//http://localhost:8088/ksql

type DropCommandStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	QueryID any    `json:"queryId"`
}

type DropInfo struct {
	Type                  string            `json:"@type"`
	StatementText         string            `json:"statementText"`
	CommandID             string            `json:"commandId"`
	CommandStatus         DropCommandStatus `json:"commandStatus"`
	CommandSequenceNumber int               `json:"commandSequenceNumber"`
	Warnings              []any             `json:"warnings"`
}

type (
	DropResponse []DropInfo
)

//[{"@type":"currentStatus","statementText":"DROP TABLE TESTIFY_D;","commandId":"table/TESTIFY_D/drop","commandStatus":{"status":"SUCCESS","message":"Source `TESTIFY_D` (topic: example_topic) was dropped.","queryId":null},"commandSequenceNumber":8,"warnings":[]}]%
//[{"@type":"currentStatus","statementText":"DROP STREAM TESTIFY;","commandId":"stream/TESTIFY/drop","commandStatus":{"status":"SUCCESS","message":"Source `TESTIFY` (topic: example_topic) was dropped.","queryId":null},"commandSequenceNumber":10,"warnings":[]}]%
