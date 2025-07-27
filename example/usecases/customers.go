package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"github.com/gulfstream-h/ksql/tables"
	"log/slog"
	"math/rand"
	"time"
)

type (
	Orders struct {
		OrderID    string    `ksql:"order_id"`
		CustomerID string    `ksql:"customer_id"`
		ItemID     string    `ksql:"item_id"`
		Quantity   int       `ksql:"quantity"`
		Price      float64   `ksql:"price"`
		OrderTime  time.Time `ksql:"order_time"`
	}

	Customers struct {
		CustomerID string `ksql:"customer_id, primary"`
		Name       string `ksql:"name"`
		Email      string `ksql:"email"`
		Region     string `ksql:"region"`
	}

	Inventory struct {
		ItemID   string `ksql:"item_id, primary"`
		ItemName string `ksql:"item_name"`
		Stock    int    `ksql:"stock"`
	}

	EnrichedOrders struct {
		OrderID    string    `ksql:"order_id"`
		CustomerID string    `ksql:"customer_id"`
		ItemID     string    `ksql:"item_id"`
		Quantity   int       `ksql:"quantity"`
		Price      float64   `ksql:"price"`
		OrderTime  time.Time `ksql:"order_time"`

		ItemName string `ksql:"item_name"`

		CustomerName string `ksql:"customer_name"`
	}

	DataPipeline struct {
		customersInput *streams.Stream[Customers]
		inventoryInput *streams.Stream[Inventory]

		orders    *streams.Stream[Orders]
		customers *tables.Table[Customers]
		inventory *tables.Table[Inventory]

		bigOrders *streams.Stream[Orders]
		enriched  *streams.Stream[EnrichedOrders]
	}
)

const (
	customersStreamName = `customers_stream`
	inventoryStreamName = `inventory_stream`

	ordersStreamName   = `orders`
	customersTableName = `customers`
	inventoryTableName = `inventory`

	bigOrdersStreamName      = `big_orders`
	enrichedOrdersStreamName = `enriched_orders`
)

func InitBase(ctx context.Context, pipe *DataPipeline) error {

	customersStream, err := streams.CreateStream[Customers](
		ctx,
		customersStreamName,
		shared.StreamSettings{
			Name:        customersStreamName,
			Partitions:  1,
			SourceTopic: customersStreamName,
		},
	)
	if err != nil {
		return fmt.Errorf("create customers stream: %w", err)
	}
	pipe.customersInput = customersStream

	inventoryStream, err := streams.CreateStream[Inventory](
		ctx,
		inventoryStreamName,
		shared.StreamSettings{
			Name:        inventoryStreamName,
			SourceTopic: inventoryStreamName,
			Partitions:  1,
		},
	)
	if err != nil {
		return fmt.Errorf("create inventory stream: %w", err)
	}

	pipe.inventoryInput = inventoryStream

	ordersStream, err := streams.CreateStream[Orders](ctx, ordersStreamName, shared.StreamSettings{
		Partitions:  1,
		SourceTopic: ordersStreamName,
		Name:        ordersStreamName,
	})
	if err != nil {
		return fmt.Errorf("create orders stream: %w", err)
	}

	pipe.orders = ordersStream

	customersTable, err := tables.CreateTable[Customers](
		ctx,
		customersTableName,
		shared.TableSettings{
			Partitions:  1,
			SourceTopic: customersStreamName,
		})
	if err != nil {
		return fmt.Errorf("create customers table: %w", err)
	}

	pipe.customers = customersTable

	inventoryTable, err := tables.CreateTable[Inventory](
		ctx,
		inventoryTableName,
		shared.TableSettings{
			Partitions:  1,
			SourceTopic: inventoryStreamName,
		})
	if err != nil {
		return fmt.Errorf("create inventory table: %w", err)
	}

	pipe.inventory = inventoryTable

	bigOrders, err := streams.CreateStreamAsSelect[Orders](
		ctx,
		bigOrdersStreamName,
		shared.StreamSettings{Partitions: 1},
		ksql.SelectAsStruct(ordersStreamName, Orders{}).
			Where(ksql.F("quantity").Greater(50)).
			From(ksql.Schema(ordersStreamName, ksql.STREAM)),
	)
	if err != nil {
		return fmt.Errorf("create big orders stream: %w", err)
	}
	pipe.bigOrders = bigOrders

	enrichedOrders, err := streams.CreateStreamAsSelect[EnrichedOrders](
		ctx,
		enrichedOrdersStreamName,
		shared.StreamSettings{Partitions: 1},
		ksql.Select(
			ksql.F("o.order_id").As("order_id"),
			ksql.F("o.customer_id").As("customer_id"),
			ksql.F("o.item_id").As("item_id"),
			ksql.F("o.quantity").As("quantity"),
			ksql.F("o.price").As("price"),
			//ksql.F("o.order_time").As("order_time"),
			ksql.F("i.item_name").As("item_name"),
			ksql.F("c.name").As("customer_name"),
		).
			From(ksql.Schema(ordersStreamName, ksql.STREAM).As("o")).
			LeftJoin(ksql.Schema(inventoryTableName, ksql.TABLE).As("i"), ksql.F("o.item_id").Equal(ksql.F("i.item_id"))).
			LeftJoin(ksql.Schema(customersTableName, ksql.TABLE).As("c"), ksql.F("o.customer_id").Equal(ksql.F("c.customer_id"))),
	)
	if err != nil {
		return fmt.Errorf("create enriched orders stream: %w", err)
	}

	pipe.enriched = enrichedOrders

	return nil

}

func dropTableLogged(ctx context.Context, name string) {
	err := tables.Drop(ctx, name)
	if err != nil {
		slog.Error(
			"DROP TABLE",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
	}
}

func dropStreamLogged(ctx context.Context, name string) {
	err := streams.Drop(ctx, name)
	if err != nil {
		slog.Error(
			"DROP STREAM",
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
	}
}

func produceOrdersLoop(ctx context.Context, stream *streams.Stream[Orders]) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context canceled, stopping produce loop...")
			return
		case <-ticker.C:
			event := Orders{
				OrderID:    fmt.Sprintf("order-%d", time.Now().UnixNano()),
				CustomerID: fmt.Sprintf("customer-%d", rand.Intn(100)),
				ItemID:     fmt.Sprintf("item-%d", rand.Intn(50)),
				Quantity:   rand.Intn(100) + 1,
				Price:      float64(rand.Intn(10000)) / 100.0,
				OrderTime:  time.Now(),
			}

			err := stream.Insert(ctx, event)
			if err != nil {
				fmt.Printf("Failed to insert event: %v\n", err)
			} else {
				fmt.Printf("Inserted event: %+v\n", event)
			}
		}
	}
}
func produceInventoryLoop(ctx context.Context, stream *streams.Stream[Inventory]) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context canceled, stopping produce loop...")
			return
		case <-ticker.C:
			event := Inventory{
				ItemID:   fmt.Sprintf("item-%d", rand.Intn(50)),
				ItemName: fmt.Sprintf("ItemName-%d", rand.Intn(50)),
				Stock:    rand.Intn(1000),
			}

			err := stream.Insert(ctx, event)
			if err != nil {
				fmt.Printf("Failed to insert event: %v\n", err)
			} else {
				fmt.Printf("Inserted event: %+v\n", event)
			}
		}
	}
}
func produceCustomersLoop(ctx context.Context, stream *streams.Stream[Customers]) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context canceled, stopping produce loop...")
			return
		case <-ticker.C:
			event := Customers{
				CustomerID: fmt.Sprintf("customer-%d", rand.Intn(100)),
				Name:       fmt.Sprintf("CustomerName-%d", rand.Intn(100)),
				Email:      fmt.Sprintf("customer%d@example.com", rand.Intn(100)),
				Region:     fmt.Sprintf("Region-%d", rand.Intn(10)),
			}

			err := stream.Insert(ctx, event)
			if err != nil {
				fmt.Printf("Failed to insert event: %v\n", err)
			} else {
				fmt.Printf("Inserted event: %+v\n", event)
			}
		}
	}
}

func listenLoop[E any](ctx context.Context, stream *streams.Stream[E]) error {
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
				fmt.Printf("Data channel closed for stream: %s\n", stream.Name)
				return nil
			}
			fmt.Printf("Event received from stream %s: %+v\n", stream.Name, evt)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	slog.SetLogLoggerLevel(slog.LevelDebug)

	err := config.
		New("http://localhost:8088", 600, true).
		Configure(ctx)

	if err != nil {
		fmt.Printf("Failed to configure ksql: %v\n", err)
		return
	}

	dropStreamLogged(ctx, bigOrdersStreamName)
	dropStreamLogged(ctx, enrichedOrdersStreamName)
	dropStreamLogged(ctx, customersStreamName)
	dropStreamLogged(ctx, inventoryStreamName)
	dropStreamLogged(ctx, ordersStreamName)
	dropTableLogged(ctx, customersTableName)
	dropTableLogged(ctx, inventoryTableName)

	pipe := &DataPipeline{}
	err = InitBase(ctx, pipe)
	if err != nil {
		fmt.Printf("Failed to initialize data pipeline: %v\n", err)
		return
	}

	go produceOrdersLoop(ctx, pipe.orders)
	go produceCustomersLoop(ctx, pipe.customersInput)
	go produceInventoryLoop(ctx, pipe.inventoryInput)

	go func() {
		err := listenLoop(ctx, pipe.bigOrders)
		if err != nil {
			fmt.Printf("Error in bigOrders listen loop: %v\n", err)
		}
	}()

	go func() {
		err := listenLoop(ctx, pipe.enriched)
		if err != nil {
			fmt.Printf("Error in enrichedOrders listen loop: %v\n", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("Application terminated")
}
