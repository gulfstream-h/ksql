package network

import (
	"errors"
	"io"
	"net/http"
)

func (n *Network) PerformRequest(
	request http.Request,
	processor processor) (
	err error) {

balance:
	var (
		rawResponse *http.Response
	)

	if n.rps.Load() == n.maxRps {
		if processor.GetType() == single {
			n.rps.Add(+1)
			n.mu.Lock()
			defer n.mu.Unlock()
			defer n.rps.Add(-1)
		} else {
			return errors.New("too many requests for long polling streams")
		}
	}

	if rawResponse, err = n.httpClient.Do(&request); err != nil {
		return err
	}

	if err = n.validateResponse(rawResponse); err != nil {
		rawResponse.Body.Close()
		return err
	}

	var (
		body []byte
	)

	if err = processor.Process(
		rawResponse,
		body,
		&n.mu,
		&n.rps); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, ErrRebalance) {
			n.mu.Lock()
			n.mu.Unlock()
			goto balance
		}

		return err
	}

	return nil
}

func (n *Network) validateResponse(response *http.Response) error {
	if response.StatusCode != http.StatusOK {
		return errors.New("invalid response")
	}

	return nil
}

var (
	ErrCannotWriteData = errors.New("cannot write info by net")
	ErrCannotReadData  = errors.New("cannot read data by net")
)
