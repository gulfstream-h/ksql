package conn

import (
	"net"
	"reflect"
	"testing"
)

func TestNewConnection(t *testing.T) {
	tests := []struct {
		name string
		want Connection
	}{
		{
			name: "TestNewConnection",
			want: &kafkaConnection{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewConnection(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewConnection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_kafkaConnection_Close(t *testing.T) {
	type fields struct {
		conn net.Conn
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Test_kafkaConnection_Close",
			fields: fields{
				conn: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := kafkaConnection{
				conn: tt.fields.conn,
			}
			if err := c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_kafkaConnection_ListStreams(t *testing.T) {
	type fields struct {
		conn net.Conn
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "Test_kafkaConnection_ListStreams",
			fields: fields{
				conn: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := kafkaConnection{
				conn: tt.fields.conn,
			}
			c.ListStreams()
		})
	}
}
