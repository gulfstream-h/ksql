package dao

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
