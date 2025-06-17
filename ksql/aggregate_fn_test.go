package ksql

import "testing"

func Test_AggregateFn(t *testing.T) {
	testcases := []struct {
		name     string
		fn       Field
		wantExpr string
		expectOK bool
	}{
		{
			name:     "Count function",
			fn:       Count(F("column1")),
			wantExpr: "COUNT(column1)",
			expectOK: true,
		},
		{
			name:     "Sum function",
			fn:       Sum(F("column2")),
			wantExpr: "SUM(column2)",
			expectOK: true,
		},
		{
			name:     "Avg function",
			fn:       Avg(F("column3")),
			wantExpr: "AVG(column3)",
			expectOK: true,
		},
		{
			name:     "Min function",
			fn:       Min(F("column4")),
			wantExpr: "MIN(column4)",
			expectOK: true,
		},
		{
			name:     "Max function",
			fn:       Max(F("column5")),
			wantExpr: "MAX(column5)",
			expectOK: true,
		},
		{
			name:     "CollectList function",
			fn:       CollectList(F("column6")),
			wantExpr: "COLLECT_LIST(column6)",
			expectOK: true,
		},
		{
			name:     "CollectSet function",
			fn:       CollectSet(F("column7")),
			wantExpr: "COLLECT_SET(column7)",
			expectOK: true,
		},
		{
			name:     "LatestByOffset function",
			fn:       LatestByOffset(F("column8")),
			wantExpr: "LATEST_BY_OFFSET(column8)",
			expectOK: true,
		},
		{
			name:     "EarliestByOffset function",
			fn:       EarliestByOffset(F("column9")),
			wantExpr: "EARLIEST_BY_OFFSET(column9)",
			expectOK: true,
		},
		{
			name:     "TopK function",
			fn:       TopK(F("column10"), 3),
			wantExpr: "TOPK(column10, 3)",
			expectOK: true,
		},
		{
			name:     "TopKDistinct function",
			fn:       TopKDistinct(F("column11"), 5),
			wantExpr: "TOPK_DISTINCT(column11, 5)",
			expectOK: true,
		},
		{
			name:     "Histogram function",
			fn:       Histogram(F("column12"), 10),
			wantExpr: "HISTOGRAM(column12, 10)",
			expectOK: true,
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			expr, ok := tt.fn.Expression()
			if ok != tt.expectOK {
				t.Errorf("expected ok=%v, got %v", tt.expectOK, ok)
				return
			}
			if expr != tt.wantExpr {
				t.Errorf("expected expression %q, got %q", tt.wantExpr, expr)
			}
		})
	}
}
