package network

import (
	"errors"
	"fmt"
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

	if rawResponse, err = n.httpClient.Do(
		&request); err != nil {
		return err
	}

	if err = n.validateResponse(
		rawResponse); err != nil {
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

		if errors.Is(
			err,
			io.EOF) {

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

var (
	ErrCannotWriteData = errors.New("cannot write info by net")
	ErrCannotReadData  = errors.New("cannot read data by net")
)

func (n *Network) validateResponse(response *http.Response) error {
	var (
		text []byte
	)

	if response.Body != nil {
		defer response.Body.Close()
		text, _ = io.ReadAll(response.Body)
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d. response: %s", response.StatusCode, text)
	}

	return nil
}
