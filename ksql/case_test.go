package ksql

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_caseExpression_Expression(t *testing.T) {
	tests := []struct {
		name    string
		expr    Expression
		want    string
		wantErr bool
	}{
		{
			name: "case 1",
			expr: Case(CaseWhen(
				F("col1").Equal("some_string"),
				"then_first",
			)).As("some_alias"),
			want: "CASE WHEN col1 = 'some_string' THEN 'then_first' ELSE NULL END AS some_alias",
		},
		{
			name: "simple case with else",
			expr: Case(CaseWhen(
				F("status").Equal("active"),
				"active_user",
			)).Else("inactive_user").As("user_status"),
			want: "CASE WHEN status = 'active' THEN 'active_user' ELSE 'inactive_user' END AS user_status",
		},

		{
			name: "case with multiple WHEN conditions",
			expr: Case(CaseWhen(
				F("age").Less(18),
				"minor",
			), CaseWhen(
				F("age").GreaterEq(18),
				"adult",
			)).As("age_group"),
			want: "CASE WHEN age < 18 THEN 'minor' WHEN age >= 18 THEN 'adult' ELSE NULL END AS age_group",
		},

		{
			name: "case with base expression (searched case)",
			expr: Case(CaseWhen(
				F("role").Equal("admin"),
				"full_access",
			), CaseWhen(
				F("role").Equal("user"),
				"limited_access",
			)).Else("no_access").As("access_level"),
			want: "CASE WHEN role = 'admin' THEN 'full_access' WHEN role = 'user' THEN 'limited_access' ELSE 'no_access' END AS access_level",
		},

		{
			name: "case with no alias",
			expr: Case(CaseWhen(
				F("score").Greater(90),
				"A",
			)).Else("F"),
			want:    "",
			wantErr: true,
		},
		{
			name: "case with numeric THEN and ELSE values",
			expr: Case(
				CaseWhen(F("score").GreaterEq(50), 1),
			).Else(0).As("pass_flag"),
			want: "CASE WHEN score >= 50 THEN 1 ELSE 0 END AS pass_flag",
		},

		{
			name: "case with NULL condition",
			expr: Case(
				CaseWhen(F("deleted_at").IsNull(), "active"),
				CaseWhen(F("deleted_at").IsNotNull(), "deleted"),
			).As("status"),
			want: "CASE WHEN deleted_at IS NULL THEN 'active' WHEN deleted_at IS NOT NULL THEN 'deleted' ELSE NULL END AS status",
		},

		{
			name:    "case with empty WHEN list (invalid)",
			expr:    Case().As("broken_case"),
			want:    "",
			wantErr: true,
		},

		{
			name: "case with boolean logic in condition",
			expr: Case(
				CaseWhen(F("is_admin").Equal(true), "admin"),
				CaseWhen(F("is_admin").Equal(false), "user"),
			).As("role_type"),
			want: "CASE WHEN is_admin = TRUE THEN 'admin' WHEN is_admin = FALSE THEN 'user' ELSE NULL END AS role_type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expression, err := tt.expr.Expression()

			assert.Equal(t, tt.wantErr, err != nil, fmt.Errorf("error message: %w", err))

			assert.Equal(t, tt.want, expression)
		})
	}
}
