package ksql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_WindowExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		w      WindowExpression
		want   string
		wantOk bool
	}{
		{
			name:   "Valid Tumbling Window",
			w:      NewTumblingWindow(TimeUnit{Val: 10, Unit: Seconds}),
			want:   "TUMBLING (SIZE 10 SECONDS)",
			wantOk: true,
		},
		{
			name:   "Invalid Tumbling Window (negative value)",
			w:      NewTumblingWindow(TimeUnit{Val: -10, Unit: Seconds}),
			want:   "",
			wantOk: false,
		},
		{
			name:   "Valid Hopping Window",
			w:      NewHoppingWindow(TimeUnit{Val: 10, Unit: Seconds}, TimeUnit{Val: 5, Unit: Seconds}),
			want:   "HOPPING (SIZE 10 SECONDS, ADVANCE BY 5 SECONDS)",
			wantOk: true,
		},
		{
			name:   "Invalid Hopping Window (zero size)",
			w:      NewHoppingWindow(TimeUnit{Val: 0, Unit: Seconds}, TimeUnit{Val: 5, Unit: Seconds}),
			want:   "",
			wantOk: false,
		},
		{
			name:   "Valid Session Window",
			w:      NewSessionWindow(TimeUnit{Val: 10, Unit: Seconds}),
			want:   "SESSION (10 SECONDS)",
			wantOk: true,
		},
		{
			name:   "Invalid Session Window (zero gap)",
			w:      NewSessionWindow(TimeUnit{Val: 0, Unit: Seconds}),
			want:   "",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := tt.w.Expression()

			assert.Equal(t, tt.wantOk, ok)
			if ok {
				assert.Equal(t, tt.want, got)
			}

		})
	}
}
