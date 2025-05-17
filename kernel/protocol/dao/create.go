package dao

// curl -X POST -H "Content-Type: application/vnd.ksql.v1+json" -d '{"ksql": "CREATE STREAM TESTIFY (ID INTEGER) WITH (KAFKA_TOPIC='\''example_topic'\'', VALUE_FORMAT='\''JSON'\'');"}' http://localhost:8088/ksql

type RelationCommandStatus struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	QueryID any    `json:"queryId"`
}

type RelationInfo struct {
	Type                  string                `json:"@type"`
	StatementText         string                `json:"statementText"`
	CommandID             string                `json:"commandId"`
	CommandStatus         RelationCommandStatus `json:"commandStatus"`
	CommandSequenceNumber int                   `json:"commandSequenceNumber"`
	Warnings              any                   `json:"warnings"`
}

type (
	CreateRelationResponse []RelationInfo
)

// error {"@type":"statement_error","error_code":40001,"message":"Cannot add table 'TESTIFY': A stream with the same name already exists","statementText":"CREATE TABLE TESTIFY (ID INTEGER) WITH (CLEANUP_POLICY='compact', KAFKA_TOPIC='example_topic', KEY_FORMAT='KAFKA', VALUE_FORMAT='JSON');","entities":[]}%
