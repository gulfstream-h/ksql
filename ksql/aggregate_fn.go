package ksql

import "strconv"

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
	Expression() (string, bool)
	Field
	Name() string
}

type aggregateFunction struct {
	name string
	Field
}

func (a *aggregateFunction) Expression() (string, bool) {
	if a.Field == nil {
		return "", false
	}

	expr, ok := a.Field.Expression()
	if !ok {
		return "", false
	}

	if len(a.name) == 0 {
		return expr, true
	}

	return a.name + "(" + expr + ")", true
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

func (t *topKFunction) Expression() (string, bool) {
	if t.aggregateFunction.Field == nil {
		return "", false
	}

	expr, ok := t.aggregateFunction.Field.Expression()
	if !ok {
		return "", false
	}

	if t.k <= 0 {
		return "", false
	}

	return t.name + "(" + expr + ", " + strconv.Itoa(t.k) + ")", true
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

func (t *topKDistinct) Expression() (string, bool) {
	if t.aggregateFunction.Field == nil {
		return "", false
	}
	expr, ok := t.aggregateFunction.Field.Expression()
	if !ok {
		return "", false
	}

	if t.k <= 0 {
		return "", false
	}

	return t.name + "(" + expr + ", " + strconv.Itoa(t.k) + ")", true
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

func (h *histogramFunction) Expression() (string, bool) {
	if h.aggregateFunction.Field == nil {
		return "", false
	}

	expr, ok := h.aggregateFunction.Field.Expression()
	if !ok {
		return "", false
	}

	if h.buckets <= 0 {
		return "", false
	}

	return h.name + "(" + expr + ", " + strconv.Itoa(h.buckets) + ")", true
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
