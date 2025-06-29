package dao

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
	CreateRelationResponse RelationInfo
)
