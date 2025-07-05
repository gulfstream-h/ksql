package ksql

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	// COUNT - aggregate function that returns quantity of all grouped rows
	COUNT = `COUNT`
	// SUM - aggregate function summarizing all grouped rows
	SUM = `SUM`
	// AVG - aggregate function that returns average value of grouped fields
	AVG = `AVG`
	// MIN - aggregate function that returns minimal value in set of grouped fields
	MIN = `MIN`
	// MAX - aggregate function that returns maximal value in set of grouped fields
	MAX = `MAX`
	// COLLECT_LIST - aggregate grouped fields into set
	COLLECT_LIST = `COLLECT_LIST`
	// COLLECT_SET - aggregate grouped fields in set
	COLLECT_SET = `COLLECT_SET`
	// LATEST_BY_OFFSET - aggregate by the latest offset
	LATEST_BY_OFFSET = `LATEST_BY_OFFSET`
	// EARLIEST_BY_OFFSET - aggregate by the earliest offset
	EARLIEST_BY_OFFSET = `EARLIEST_BY_OFFSET`

	// parameterized

	TOPK          = `TOPK`
	TOPK_DISTINCT = `TOPK_DISTINCT`
	HISTOGRAM     = `HISTOGRAM`
)

// AggregateFunction - common contract for introspecting fields
// that must be aggregated
type AggregateFunction interface {
	Expression() (string, error)
	Field
	Name() string
}

// aggregateFunction - basement structure of all aggregated fields realizations
type aggregateFunction struct {
	name string
	Field
}

// Expression - accumulates all applied settings and build string query
func (a *aggregateFunction) Expression() (string, error) {
	if a.Field == nil {
		return "", fmt.Errorf("aggregate function %s requires a field", a.name)
	}

	expr, err := a.Field.Expression()
	if err != nil {
		return "", fmt.Errorf("field expression: %w", err)
	}

	if len(a.name) == 0 {
		return expr, errors.New("aggregate function name cannot be empty")
	}

	return a.name + "(" + expr + ")", nil
}

// Name - returns function name of aggregate field. Like MAX, MIN, AVG etc...
func (a *aggregateFunction) Name() string {
	return a.name
}

// Count returns COUNT(field) wrapper
func Count(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COUNT,
		Field: f,
	})
}

// Sum returns Sum(field) wrapper
func Sum(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  SUM,
		Field: f,
	})
}

// Avg returns Avg(field) wrapper
func Avg(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  AVG,
		Field: f,
	})
}

// Min returns Min(field) wrapper
func Min(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  MIN,
		Field: f,
	})
}

// Max returns Max(field) wrapper
func Max(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  MAX,
		Field: f,
	})
}

// CollectList returns CollectList(field) wrapper
func CollectList(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COLLECT_LIST,
		Field: f,
	})
}

// CollectSet returns CollectSet(field) wrapper
func CollectSet(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COLLECT_SET,
		Field: f,
	})
}

// LatestByOffset returns LatestByOffset(field) wrapper
func LatestByOffset(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  LATEST_BY_OFFSET,
		Field: f,
	})
}

// EarliestByOffset returns EarliestByOffset(field) wrapper
func EarliestByOffset(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  EARLIEST_BY_OFFSET,
		Field: f,
	})
}

// topKFunction - realization of histogram aggregation mechanism
type topKFunction struct {
	aggregateFunction
	k int
}

// Expression - accumulates all applied settings and build string query
func (t *topKFunction) Expression() (string, error) {
	if t.aggregateFunction.Field == nil {
		return "", fmt.Errorf("topK function requires a field")
	}

	expr, err := t.aggregateFunction.Field.Expression()
	if err != nil {
		return "", fmt.Errorf("field expression: %w", err)
	}

	if t.k <= 0 {
		return "", fmt.Errorf("topK function requires k to be greater than 0")
	}

	return t.name + "(" + expr + ", " + strconv.Itoa(t.k) + ")", nil
}

// TopK - returns aggregated ksql TopK field, that must be configured
// in exact useCase purposes
func TopK(f Field, k int) Field {
	return NewAggregatedField(&topKFunction{
		aggregateFunction: aggregateFunction{
			name:  TOPK,
			Field: f,
		},
		k: k,
	})
}

// topKDistinct - realization of histogram aggregation mechanism
type topKDistinct struct {
	aggregateFunction
	k int
}

// Expression - accumulates all applied settings and build string query
func (t *topKDistinct) Expression() (string, error) {
	if t.aggregateFunction.Field == nil {
		return "", fmt.Errorf("topKDistinct function requires a field")
	}
	expr, err := t.aggregateFunction.Field.Expression()
	if err != nil {
		return "", fmt.Errorf("field expression: %w", err)
	}

	if t.k <= 0 {
		return "", fmt.Errorf("topKDistinct function requires k to be greater than 0")
	}

	return t.name + "(" + expr + ", " + strconv.Itoa(t.k) + ")", nil
}

// TopKDistinct - returns aggregated ksql topKDistinct field, that must be configured
// in exact useCase purposes
func TopKDistinct(f Field, k int) Field {
	return NewAggregatedField(&topKDistinct{
		aggregateFunction: aggregateFunction{
			name:  TOPK_DISTINCT,
			Field: f,
		},
		k: k,
	})
}

// histogramFunction - realization of histogram aggregation mechanism
type histogramFunction struct {
	aggregateFunction
	buckets int
}

// Expression - accumulates all applied settings and build string query
func (h *histogramFunction) Expression() (string, error) {
	if h.aggregateFunction.Field == nil {
		return "", fmt.Errorf("histogram function requires a field")
	}

	expr, err := h.aggregateFunction.Field.Expression()
	if err != nil {
		return "", fmt.Errorf("field expression: %w", err)
	}

	if h.buckets <= 0 {
		return "", fmt.Errorf("histogram function requires buckets to be greater than 0")
	}

	return h.name + "(" + expr + ", " + strconv.Itoa(h.buckets) + ")", nil
}

// Histogram - returns aggregated ksql histogram field, that must be configured
// in exact useCase purposes
func Histogram(f Field, buckets int) Field {
	return NewAggregatedField(&histogramFunction{
		aggregateFunction: aggregateFunction{
			name:  HISTOGRAM,
			Field: f,
		},
		buckets: buckets,
	})
}
