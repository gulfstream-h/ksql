package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_AggregateFn(t *testing.T) {
	testcases := []struct {
		name      string
		fn        Field
		wantExpr  string
		expectErr bool
	}{
		{
			name:      "Count function",
			fn:        Count(F("column1")),
			wantExpr:  "COUNT(column1)",
			expectErr: false,
		},
		{
			name:      "Sum function",
			fn:        Sum(F("column2")),
			wantExpr:  "SUM(column2)",
			expectErr: false,
		},
		{
			name:      "Avg function",
			fn:        Avg(F("column3")),
			wantExpr:  "AVG(column3)",
			expectErr: false,
		},
		{
			name:      "Min function",
			fn:        Min(F("column4")),
			wantExpr:  "MIN(column4)",
			expectErr: false,
		},
		{
			name:      "Max function",
			fn:        Max(F("column5")),
			wantExpr:  "MAX(column5)",
			expectErr: false,
		},
		{
			name:      "CollectList function",
			fn:        CollectList(F("column6")),
			wantExpr:  "COLLECT_LIST(column6)",
			expectErr: false,
		},
		{
			name:      "CollectSet function",
			fn:        CollectSet(F("column7")),
			wantExpr:  "COLLECT_SET(column7)",
			expectErr: false,
		},
		{
			name:      "LatestByOffset function",
			fn:        LatestByOffset(F("column8")),
			wantExpr:  "LATEST_BY_OFFSET(column8)",
			expectErr: false,
		},
		{
			name:      "EarliestByOffset function",
			fn:        EarliestByOffset(F("column9")),
			wantExpr:  "EARLIEST_BY_OFFSET(column9)",
			expectErr: false,
		},
		{
			name:      "TopK function",
			fn:        TopK(F("column10"), 3),
			wantExpr:  "TOPK(column10, 3)",
			expectErr: false,
		},
		{
			name:      "TopKDistinct function",
			fn:        TopKDistinct(F("column11"), 5),
			wantExpr:  "TOPK_DISTINCT(column11, 5)",
			expectErr: false,
		},
		{
			name:      "Histogram function",
			fn:        Histogram(F("column12"), 10),
			wantExpr:  "HISTOGRAM(column12, 10)",
			expectErr: false,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			expr, err := tt.fn.Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.wantExpr, expr)
			}
		})
	}
}
