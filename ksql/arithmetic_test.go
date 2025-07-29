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
			expr:    Add(F("col"), F("col2")),
			wantErr: false,
			want:    `( col + col2 )`,
		},
		{
			name:    "add field and constant",
			expr:    Add(F("col"), 5),
			wantErr: false,
			want:    `( col + 5 )`,
		},
		{
			name:    "subtract two fields",
			expr:    Sub(F("a"), F("b")),
			wantErr: false,
			want:    `( a - b )`,
		},
		{
			name:    "multiply field and constant",
			expr:    Mul(F("price"), 2),
			wantErr: false,
			want:    `( price * 2 )`,
		},
		{
			name:    "divide field by field",
			expr:    Div(F("total"), F("count")),
			wantErr: false,
			want:    `( total / count )`,
		},
		{
			name:    "divide by zero constant",
			expr:    Div(F("x"), 0),
			wantErr: false,
			want:    `( x / 0 )`,
		},
		{
			name:    "invalid operand type",
			expr:    Add(F("x"), struct{}{}),
			wantErr: true,
			want:    "",
		},
		{
			name:    "nested expression: (a + b) * c",
			expr:    Mul(Add(F("a"), F("b")), F("c")),
			wantErr: false,
			want:    `( ( a + b ) * c )`,
		},
		{
			name:    "nested with constant: (a - 3) / 2",
			expr:    Div(Sub(F("a"), 3), 2),
			wantErr: false,
			want:    `( ( a - 3 ) / 2 )`,
		},
		{
			name:    "deep nesting: ((a + b) * (c - d))",
			expr:    Mul(Add(F("a"), F("b")), Sub(F("c"), F("d"))),
			wantErr: false,
			want:    `( ( a + b ) * ( c - d ) )`,
		},
		{
			name:    "mixed types: ((a + 1) * (2 - b))",
			expr:    Mul(Add(F("a"), 1), Sub(2, F("b"))),
			wantErr: false,
			want:    `( ( a + 1 ) * ( 2 - b ) )`,
		},
		{
			name:    "complex with constants: ((x + 1) * (y - 2) / 3)",
			expr:    Div(Mul(Add(F("x"), 1), Sub(F("y"), 2)), 3),
			wantErr: false,
			want:    `( ( ( x + 1 ) * ( y - 2 ) ) / 3 )`,
		},
		{
			name:    "arithmetic with aggregate func",
			expr:    Mul(Mul(Sum(F("col1")), F("col2")), 0.005),
			wantErr: false,
			want:    `( ( SUM(col1) * col2 ) * 0.005 )`,
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
