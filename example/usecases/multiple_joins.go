package usecases

import (
	"context"
	"fmt"
	"ksql/ksql"
	"ksql/shared"
	"ksql/streams"
	"ksql/tables"
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
		CustomerID string `ksql:"customer_id"`
		Name       string `ksql:"name"`
		Email      string `ksql:"email"`
		Region     string `ksql:"region"`
	}

	Inventory struct {
		ItemID   string `ksql:"item_id"`
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
		orders    *streams.Stream[Orders]
		customers *tables.Table[Customers]
		inventory *tables.Table[Inventory]

		bigOrders *streams.Stream[Orders]
		enriched  *streams.Stream[EnrichedOrders]
	}
)

const (
	ordersStreamName   = `orders`
	customersTableName = `customers`
	inventoryTableName = `inventory`

	bigOrdersStreamName      = `big_orders`
	enrichedOrdersStreamName = `enriched_orders`
)

func InitBase(ctx context.Context, pipe *DataPipeline) error {

	ordersStream, err := streams.CreateStream[Orders](ctx, ordersStreamName, shared.StreamSettings{
		Partitions: 1,
	})
	if err != nil {
		return fmt.Errorf("create orders stream: %w", err)
	}

	pipe.orders = ordersStream

	customersTable, err := tables.CreateTable[Customers](ctx, customersTableName, shared.TableSettings{Partitions: 1})
	if err != nil {
		return fmt.Errorf("create customers table: %w", err)
	}

	pipe.customers = customersTable

	inventoryTable, err := tables.CreateTable[Inventory](ctx, inventoryTableName, shared.TableSettings{Partitions: 1})
	if err != nil {
		return fmt.Errorf("create inventory table: %w", err)
	}

	pipe.inventory = inventoryTable

	bigOrders, err := streams.CreateStreamAsSelect[Orders](
		ctx,
		bigOrdersStreamName,
		shared.StreamSettings{Partitions: 1},
		ksql.SelectAsStruct(ordersStreamName, &Orders{}).
			Where(ksql.F("quantity").Greater(50)),
	)

	pipe.bigOrders = bigOrders

	enrichedOrders, err := streams.CreateStreamAsSelect[EnrichedOrders](
		ctx,
		enrichedOrdersStreamName,
		shared.StreamSettings{Partitions: 1},
		ksql.Select(
			ksql.F("o.order_id"),
			ksql.F("o.customer_id"),
			ksql.F("o.item_id"),
			ksql.F("o.quantity"),
			ksql.F("o.price"),
			ksql.F("o.order_time"),
			ksql.F("i.item_name"),
			ksql.F("c.name").As("customer_name"),
		).
			From(ordersStreamName, ksql.STREAM).
			As("o").
			LeftJoin(inventoryTableName, ksql.F("o.item_id").Equal(ksql.F("i.item_id"))).
			LeftJoin(customersTableName, ksql.F("o.customer_id").Equal(ksql.F("c.customer_id"))),
	)

	pipe.enriched = enrichedOrders

	return nil

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
func produceInventoryLoop(ctx context.Context, stream *tables.Table[Inventory]) {
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
func produceCustomersLoop(ctx context.Context, stream *tables.Table[Customers]) {
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
	dataChan, err := stream.SelectWithEmit(ctx)
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

	pipe := &DataPipeline{}
	err := InitBase(ctx, pipe)
	if err != nil {
		fmt.Printf("Failed to initialize data pipeline: %v\n", err)
		return
	}

	go produceOrdersLoop(ctx, pipe.orders)
	go produceCustomersLoop(ctx, pipe.customers)
	go produceInventoryLoop(ctx, pipe.inventory)

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
