package database

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"ksql/internal/schema/netparse"
	"ksql/kernel/network"
	"ksql/kernel/protocol/dao"
	"log/slog"
	"net/http"
	"strings"
)

func Execute(
	ctx context.Context,
	query string,
) (string, error) {
	response, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		network.ShortPolling{},
	)
	if err != nil {
		return "", err
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case msg := <-response:
		return string(msg), nil
	}
}

func Select[S any](
	ctx context.Context,
	query string,
) (<-chan S, error) {

	valuesC := make(chan S)

	response, err := network.Net.PerformSelect(
		ctx,
		http.MethodPost,
		query,
		network.LongPolling{},
	)
	if err != nil {
		return nil, err
	}

	go func() {
		var (
			iter    = 0
			headers dao.Header
		)

		for {
			select {
			case <-ctx.Done():
				close(valuesC)
				return
			case val, ok := <-response:
				if !ok {
					close(valuesC)
					return
				}

				if strings.Contains(string(val), "Query Completed") {
					close(valuesC)
					return
				}

				if iter == 0 {
					str := val[1 : len(val)-1]

					if err = jsoniter.Unmarshal(str, &headers); err != nil {
						slog.Debug("unmarshal resonse", "formatted", string(str))
						close(valuesC)
						return
					}

					iter++
					continue
				}

				var (
					row dao.Row
				)

				if err = jsoniter.Unmarshal(val[:len(val)-1], &row); err != nil {
					close(valuesC)
					return
				}
				value, err := netparse.ParseNetResponse[S](headers, row)
				if err != nil {
					close(valuesC)
					slog.Error(
						"parse net response",
						slog.String("error", err.Error()),
						slog.Any("headers", headers),
						slog.Any("row", row),
					)
					return
				}

				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}
