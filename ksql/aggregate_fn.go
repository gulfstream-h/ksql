package ksql

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	COUNT              = `COUNT`
	SUM                = `SUM`
	AVG                = `AVG`
	MIN                = `MIN`
	MAX                = `MAX`
	COLLECT_LIST       = `COLLECT_LIST`
	COLLECT_SET        = `COLLECT_SET`
	LATEST_BY_OFFSET   = `LATEST_BY_OFFSET`
	EARLIEST_BY_OFFSET = `EARLIEST_BY_OFFSET`

	// parameterized

	TOPK          = `TOPK`
	TOPK_DISTINCT = `TOPK_DISTINCT`
	HISTOGRAM     = `HISTOGRAM`
)

type AggregateFunction interface {
	Expression() (string, error)
	Field
	Name() string
}

type aggregateFunction struct {
	name string
	Field
}

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

func (a *aggregateFunction) Name() string {
	return a.name
}

func Count(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COUNT,
		Field: f,
	})
}
func Sum(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  SUM,
		Field: f,
	})
}

func Avg(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  AVG,
		Field: f,
	})
}

func Min(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  MIN,
		Field: f,
	})
}

func Max(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  MAX,
		Field: f,
	})
}

func CollectList(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COLLECT_LIST,
		Field: f,
	})
}

func CollectSet(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  COLLECT_SET,
		Field: f,
	})
}

func LatestByOffset(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  LATEST_BY_OFFSET,
		Field: f,
	})
}

func EarliestByOffset(f Field) Field {
	return NewAggregatedField(&aggregateFunction{
		name:  EARLIEST_BY_OFFSET,
		Field: f,
	})
}

type topKFunction struct {
	aggregateFunction
	k int
}

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

func TopK(f Field, k int) Field {
	return NewAggregatedField(&topKFunction{
		aggregateFunction: aggregateFunction{
			name:  TOPK,
			Field: f,
		},
		k: k,
	})
}

type topKDistinct struct {
	aggregateFunction
	k int
}

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

func TopKDistinct(f Field, k int) Field {
	return NewAggregatedField(&topKDistinct{
		aggregateFunction: aggregateFunction{
			name:  TOPK_DISTINCT,
			Field: f,
		},
		k: k,
	})
}

type histogramFunction struct {
	aggregateFunction
	buckets int
}

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

func Histogram(f Field, buckets int) Field {
	return NewAggregatedField(&histogramFunction{
		aggregateFunction: aggregateFunction{
			name:  HISTOGRAM,
			Field: f,
		},
		buckets: buckets,
	})
}
