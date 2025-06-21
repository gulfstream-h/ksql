package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_WindowExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		w         WindowExpression
		want      string
		expectErr bool
	}{
		{
			name:      "Valid Tumbling Window",
			w:         NewTumblingWindow(TimeUnit{Val: 10, Unit: Seconds}),
			want:      "WINDOW TUMBLING (SIZE 10 SECONDS)",
			expectErr: false,
		},
		{
			name:      "Invalid Tumbling Window (negative value)",
			w:         NewTumblingWindow(TimeUnit{Val: -10, Unit: Seconds}),
			want:      "",
			expectErr: true,
		},
		{
			name:      "Valid Hopping Window",
			w:         NewHoppingWindow(TimeUnit{Val: 10, Unit: Seconds}, TimeUnit{Val: 5, Unit: Seconds}),
			want:      "WINDOW HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)",
			expectErr: false,
		},
		{
			name:      "Invalid Hopping Window (zero size)",
			w:         NewHoppingWindow(TimeUnit{Val: 0, Unit: Seconds}, TimeUnit{Val: 5, Unit: Seconds}),
			want:      "",
			expectErr: true,
		},
		{
			name:      "Valid Session Window",
			w:         NewSessionWindow(TimeUnit{Val: 10, Unit: Seconds}),
			want:      "WINDOW SESSION (10 SECONDS)",
			expectErr: false,
		},
		{
			name:      "Invalid Session Window (zero gap)",
			w:         NewSessionWindow(TimeUnit{Val: 0, Unit: Seconds}),
			want:      "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.w.Expression()

			assert.Equal(t, tt.expectErr, err != nil)
			if !tt.expectErr {
				assert.Equal(t, tt.want, got)
			}

		})
	}
}
