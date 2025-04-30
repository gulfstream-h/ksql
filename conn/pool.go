package conn

import "sync"

type Pool struct {
	mu                sync.Mutex
	brokerConnections map[uint]Connection
}

func (p *Pool) GetConnection(brokerID uint) Connection {
	if conn, ok := p.brokerConnections[brokerID]; ok {
		return conn
	}
	return nil
}
