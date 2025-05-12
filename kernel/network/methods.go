package network

import (
	"errors"
	"io"
	"net/http"
)

func (n *Network) PerformRequest(
	request *http.Request,
	processor processor) {

balance:

	if n.rps.Load() == n.maxRps {
		if processor.GetType() == single {
			n.rps.Add(+1)
			n.mu.Lock()
			defer n.mu.Unlock()
			defer n.rps.Add(-1)
		} else {
			return
		}
	}

	rawResponse, err := n.httpClient.Do(
		request)
	if err != nil {
		return
	}

	var (
		body []byte
	)

	if err = processor.Process(
		rawResponse,
		body,
		&n.rps); err != nil {

		if errors.Is(
			err,
			io.EOF) {

			return
		}

		if errors.Is(err, ErrRebalance) {
			n.mu.Lock()
			n.mu.Unlock()
			goto balance
		}

		return
	}

	return
}
