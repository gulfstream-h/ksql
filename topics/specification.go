package topics

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	libErrors "ksql/errors"
	"ksql/internal/kernel/network"
	"ksql/internal/kernel/protocol/dao"
	"ksql/internal/kernel/protocol/dto"
	"ksql/ksql"
	"net/http"
)

// ListTopics - returns all existing topics with metadata
func ListTopics(ctx context.Context) (dto.ShowTopics, error) {
	query, _ := ksql.List(ksql.TOPIC).Expression()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.ShowTopics{}, err
	}

	select {
	case <-ctx.Done():
		return dto.ShowTopics{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowTopics{}, libErrors.ErrMalformedResponse
		}

		var (
			topics []dao.ShowTopics
		)

		if err = jsoniter.Unmarshal(val, &topics); err != nil {
			err = errors.Join(libErrors.ErrUnserializableResponse, err)
			return dto.ShowTopics{}, err
		}

		if len(topics) == 0 {
			return dto.ShowTopics{}, errors.New("no topics have been found")
		}

		return topics[0].DTO(), nil
	}
}
