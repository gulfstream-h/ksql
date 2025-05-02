package network

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

type (
	SingeHandler  struct{ maxRPS int32 }
	SocketHandler struct{ maxRPS int32 }
)

const (
	single = "single"
	socket = "socket"
)

var (
	ErrRebalance = errors.New("too many requests for long polling streams. Rebalance in second...")
)

func (sr *SingeHandler) Process(
	resp *http.Response,
	dst []byte,
	_ *sync.Mutex,
	rps *atomic.Int32) (err error) {

	defer rps.Add(-1)
	defer resp.Body.Close()

	dst, err = io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	fmt.Printf("Received data: %s\n", dst)

	return nil
}

func (sr *SingeHandler) GetType() string {
	return single
}

func (sr *SocketHandler) Process(
	resp *http.Response,
	dst []byte,
	_ *sync.Mutex,
	cps *atomic.Int32) error {

	defer cps.Add(-1)
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()

		if len(line) == 0 {
			if cps.Load() == sr.maxRPS {
				return ErrRebalance
			}

			continue
		}

		dst = []byte(line)
		if cps.Load() == sr.maxRPS {
			return ErrRebalance
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	return nil
}

func (sr *SocketHandler) GetType() string {
	return socket
}

type processor interface {
	Process(*http.Response, []byte, *sync.Mutex, *atomic.Int32) error
	GetType() string
}
