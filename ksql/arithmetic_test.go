package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_arithmeticExpr_Expression(t *testing.T) {
	type args struct {
		left  Field
		right any
		op    ArithmeticOperation
	}

	tests := []struct {
		name    string
		args    args
		expr    Expression
		want    string
		wantErr bool
	}{
		{
			name:    "case 1",
			expr:    F("col").Add(F("col2")),
			wantErr: false,
			want:    `( col + col2 )`,
		},
		{
			name:    "add field and constant",
			expr:    F("col").Add(5),
			wantErr: false,
			want:    `( col + 5 )`,
		},
		{
			name:    "subtract two fields",
			expr:    F("a").Sub(F("b")),
			wantErr: false,
			want:    `( a - b )`,
		},
		{
			name:    "multiply field and constant",
			expr:    F("price").Mul(2),
			wantErr: false,
			want:    `( price * 2 )`,
		},
		{
			name:    "divide field by field",
			expr:    F("total").Div(F("count")),
			wantErr: false,
			want:    `( total / count )`,
		},
		{
			name:    "divide by zero constant",
			expr:    F("x").Div(0),
			wantErr: false,
			want:    `( x / 0 )`,
		},
		{
			name:    "invalid operand type",
			expr:    F("x").Add(struct{}{}),
			wantErr: true,
			want:    "",
		},
		{
			name: "nested expression: (a + b) * c",
			//expr: F("a").Add(F("b").Mul(F("c"))),
			//expr:    F("c").Mul(F("a").Add(F("b"))),
			expr:    F("a").Add(F("b")).Mul("c"),
			wantErr: false,
			want:    `( ( a + b ) * c )`,
		},
		{
			name:    "nested with constant: (a - 3) / 2",
			expr:    F("a").Sub(3).Div(2),
			wantErr: false,
			want:    `( ( a - 3 ) / 2 )`,
		},
		{
			name:    "deep nesting: ((a + b) * (c - d))",
			expr:    F("a").Add(F("b")).Mul(F("c").Sub(F("d"))),
			wantErr: false,
			want:    `( ( a + b ) * ( c - d ) )`,
		},
		{
			name:    "mixed types: ((a + 1) * (2 - b))",
			expr:    F("a").Add(1).Mul(2).Sub(F("b")),
			wantErr: false,
			want:    `( ( ( a + 1 ) * 2 ) - b )`,
		},
		{
			name:    "complex with constants: ((x + 1) * (y - 2) / 3)",
			expr:    F("x").Add(1).Mul(F("y").Sub(2)).Div(3),
			wantErr: false,
			want:    `( ( ( x + 1 ) * ( y - 2 ) ) / 3 )`,
		},
		{
			name:    "arithmetic with aggregate func",
			expr:    Sum(F("col1")).Mul(F("col2")).Mul(0.005),
			wantErr: false,
			want:    `( ( SUM( col1 ) * col2 ) * 0.005 )`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := tt.expr.Expression()
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.want, expression)
		})
	}
}
