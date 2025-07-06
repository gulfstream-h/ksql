package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/static"
	"github.com/gulfstream-h/ksql/streams"
	"log"
	"log/slog"
	"math/rand"
	"os/signal"
	"syscall"
	"time"
)

type (
	Purchase struct {
		PurchaseID      int64  `ksql:"purchase_id"`
		PurchaseAmount  int64  `ksql:"purchase_amount"`
		DiscountPercent int64  `ksql:"discount_percent"`
		ShopName        string `ksql:"shop_name"`
		BuyerID         int64  `ksql:"buyer_id"`
		SellerID        int64  `ksql:"seller_id"`
	}

	Discounts struct {
		PurchaseID      int64 `ksql:"purchase_id"`
		DiscountPercent int64 `ksql:"discount_percent"`
	}

	SellerPurchases struct {
		PurchaseID     int64 `ksql:"purchase_id"`
		PurchaseAmount int64 `ksql:"purchase_amount"`
		SellerID       int64 `ksql:"seller_id"`
	}

	BuyerHistory struct {
		PurchaseID     int64 `ksql:"purchase_id"`
		PurchaseAmount int64 `ksql:"purchase_amount"`
		BuyerID        int64 `ksql:"buyer_id"`
	}

	DataPipeline struct {
		purchases       *streams.Stream[Purchase]
		discounts       *streams.Stream[Discounts]
		sellerPurchases *streams.Stream[SellerPurchases]
		buyerHistory    *streams.Stream[BuyerHistory]
	}
)

var (
	shops = []string{`ShopA`, `ShopB`, `ShopC`, `ShopD`, `ShopE`}
)

func NewDataPipeline(ctx context.Context) (*DataPipeline, error) {

	const (
		purchasesStreamName = `purchases_stream`
		purchasesTopicName  = `purchases_topic`

		discountsStreamName = `discounts_stream`

		sellerPurchasesStreamName = `seller_purchases`

		buyerHistoryStreamName = `buyer_history`
	)

	purchasesStream, err := streams.CreateStream[Purchase](
		ctx,
		purchasesStreamName,
		shared.StreamSettings{
			Name:        purchasesStreamName,
			SourceTopic: purchasesTopicName,
			Partitions:  1,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create stream: %w", err)
	}

	discountsStream, err := streams.CreateStreamAsSelect[Discounts](
		ctx,
		discountsStreamName,
		shared.StreamSettings{
			Name:       discountsStreamName,
			Partitions: 1,
		},
		ksql.
			SelectAsStruct(purchasesStreamName, &Discounts{}).
			From(ksql.Schema(purchasesStreamName, ksql.STREAM)),
	)

	if err != nil {
		return nil, fmt.Errorf("create discounts stream as select: %w", err)
	}

	sellerPurchasesStream, err := streams.CreateStreamAsSelect[SellerPurchases](
		ctx,
		sellerPurchasesStreamName,
		shared.StreamSettings{
			Name:       sellerPurchasesStreamName,
			Partitions: 1,
		},
		ksql.
			SelectAsStruct(purchasesStreamName, &SellerPurchases{}).
			From(ksql.Schema(purchasesStreamName, ksql.STREAM)),
	)

	if err != nil {
		return nil, fmt.Errorf("create seller purchases stream as select: %w", err)
	}

	buyerHistory, err := streams.CreateStreamAsSelect[BuyerHistory](
		ctx,
		buyerHistoryStreamName,
		shared.StreamSettings{
			Name:       buyerHistoryStreamName,
			Partitions: 1,
		},
		ksql.
			SelectAsStruct(purchasesStreamName, &BuyerHistory{}).
			From(ksql.Schema(purchasesStreamName, ksql.STREAM)),
	)

	if err != nil {
		return nil, fmt.Errorf("create buyer history stream as select: %w", err)
	}

	return &DataPipeline{
		purchases:       purchasesStream,
		discounts:       discountsStream,
		sellerPurchases: sellerPurchasesStream,
		buyerHistory:    buyerHistory,
	}, nil

}

func close(ctx context.Context, streamName string) {
	err := streams.Drop(ctx, streamName)
	if err != nil {
		slog.Error(
			"drop stream",
			slog.String("error", err.Error()),
			slog.String("stream_name", streamName),
		)
	}

	slog.Info(
		"stream successfully dropped",
		slog.String("stream_name", streamName),
	)
}

func (dp *DataPipeline) Close() {
	ctx := context.Background()
	close(ctx, dp.purchases.Name)
	close(ctx, dp.discounts.Name)
	close(ctx, dp.buyerHistory.Name)
	close(ctx, dp.sellerPurchases.Name)
}

func Init(ctx context.Context) error {
	const (
		host = `http://localhost:8088`
	)

	slog.SetLogLoggerLevel(slog.LevelDebug)

	err := config.
		New(host, 600, true).
		Configure(ctx)
	if err != nil {
		return fmt.Errorf("init config: %w", err)
	}

	return nil
}

func produceLoop(
	ctx context.Context,
	stream *streams.Stream[Purchase],
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("context deadline exceeded... exiting")
			return
		case <-ticker.C:
			event := Purchase{
				PurchaseID:      rand.Int63(),
				PurchaseAmount:  rand.Int63(),
				DiscountPercent: int64(rand.Intn(50)),
				ShopName:        shops[rand.Intn(len(shops))],
				BuyerID:         int64(rand.Intn(100)),
				SellerID:        int64(rand.Intn(100) + 100),
			}

			err := stream.Insert(ctx, event)
			if err != nil {
				slog.Error(
					"insert to stream",
					slog.String("error", err.Error()),
					slog.Any("event", event),
				)
			}

			slog.Info("inserted", slog.Int64("purchaseID", event.PurchaseID))

		}
	}
}

func listenLoop[E any](
	ctx context.Context,
	stream *streams.Stream[E],
) error {
	dataChan, _, err := stream.SelectWithEmit(ctx)
	if err != nil {
		return fmt.Errorf("select with emit: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case evt, ok := <-dataChan:
			if !ok {
				slog.Warn(
					"data chan closed",
					slog.String("stream_name", stream.Name),
				)
			}
			slog.Info(
				"event received",
				slog.String("stream_name", stream.Name),
				slog.Any("event", evt),
			)
		}
	}
}

func main() {
	static.ReflectionFlag = true
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	err := Init(ctx)
	if err != nil {
		log.Printf("init: %s", err.Error())
		return
	}
	streams.Drop(ctx, "purchases_stream")
	streams.Drop(ctx, "discounts_stream")
	streams.Drop(ctx, "seller_purchases")
	streams.Drop(ctx, "buyer_history")

	pipeline, err := NewDataPipeline(ctx)
	if err != nil {
		log.Printf("new data pipeline: %s", err.Error())
		return
	}
	defer pipeline.Close()

	go produceLoop(ctx, pipeline.purchases)

	go func() {
		err = listenLoop(ctx, pipeline.purchases)
		if err != nil {
			slog.Error(
				"init listen loop for purchases",
				slog.String("error", err.Error()),
			)
		}
	}()

	go func() {
		err = listenLoop(ctx, pipeline.discounts)
		if err != nil {
			slog.Error(
				"init listen loop for discounts",
				slog.String("error", err.Error()),
			)
		}
	}()
	go func() {
		err = listenLoop(ctx, pipeline.sellerPurchases)
		if err != nil {
			slog.Error(
				"init listen loop for seller purchases",
				slog.String("error", err.Error()),
			)
		}
	}()
	go func() {
		err = listenLoop(ctx, pipeline.buyerHistory)
		if err != nil {
			slog.Error(
				"init listen loop for buyer history",
				slog.String("error", err.Error()),
			)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("TERMINATED")
	}

}
