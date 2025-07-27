package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/example/usecases/purchases_flow/dtypes"
	"github.com/gulfstream-h/ksql/example/usecases/purchases_flow/utils"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"github.com/gulfstream-h/ksql/tables"
	"log/slog"
)

type (
	Dictionary struct {
		ProductsInput  *streams.Stream[dtypes.Product]
		Products       *tables.Table[dtypes.Product]
		ShopsInput     *streams.Stream[dtypes.Shop]
		Shops          *tables.Table[dtypes.Shop]
		EmployeesInput *streams.Stream[dtypes.Employee]
		Employees      *tables.Table[dtypes.Employee]
		CustomersInput *streams.Stream[dtypes.Customer]
		Customers      *tables.Table[dtypes.Customer]
	}

	PurchasesPipeline struct {
		dictionary         *Dictionary
		purchases          *streams.Stream[dtypes.Purchase]
		bonusInvoices      *streams.Stream[dtypes.BonusInvoice]
		bonusBalances      *tables.Table[dtypes.BonusBalance]
		bonusLevels        *tables.Table[dtypes.BonusLevel]
		regionAnalytics    *tables.Table[dtypes.RegionAnalytics]
		sellerKPI          *tables.Table[dtypes.SellerKPI]
		sellerSalary       *tables.Table[dtypes.SellerSalary]
		favoriteCategories *tables.Table[dtypes.FavoriteCategory]
		notifications      *tables.Table[dtypes.UserNotification]
	}
)

func NewPipeline(ctx context.Context) (*PurchasesPipeline, error) {
	pipe := new(PurchasesPipeline)

	/*
		At first, we should create all necessary tables and streams
		for join it to streams/tables in the pipeline
	*/

	// Products input stream
	productsInputStream, err := streams.CreateStream[dtypes.Product](ctx, "products_stream", shared.StreamSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create products stream: %w", err)
	}

	// Shops input stream
	shopsInputStream, err := streams.CreateStream[dtypes.Shop](ctx, "shops_stream", shared.StreamSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create shops stream: %w", err)
	}

	// Employees input stream
	employeesInputStream, err := streams.CreateStream[dtypes.Employee](ctx, "employees_stream", shared.StreamSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create employees stream: %w", err)
	}

	// Customers input stream
	customersInputStream, err := streams.CreateStream[dtypes.Customer](ctx, "customers_stream", shared.StreamSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create customers stream: %w", err)
	}

	// Shops dictionary
	shopsTable, err := tables.CreateTable[dtypes.Shop](ctx, "shops_table", shared.TableSettings{
		SourceTopic: "shops_stream",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create shops table: %w", err)
	}

	// Products dictionary
	productsTable, err := tables.CreateTable[dtypes.Product](ctx, "products_table", shared.TableSettings{
		Partitions:  1,
		SourceTopic: "products_stream",
	})
	if err != nil {
		return nil, fmt.Errorf("create products table: %w", err)
	}

	// Employees dictionary
	employeesTable, err := tables.CreateTable[dtypes.Employee](ctx, "employees_table", shared.TableSettings{
		Partitions:  1,
		SourceTopic: "employees_stream",
	})
	if err != nil {
		return nil, fmt.Errorf("create employees table: %w", err)
	}

	// Customers dictionary
	customersTable, err := tables.CreateTable[dtypes.Customer](ctx, "customers_table", shared.TableSettings{
		Partitions:  1,
		SourceTopic: "customers_stream",
	})
	if err != nil {
		return nil, fmt.Errorf("create customers table: %w", err)
	}

	pipe.dictionary = &Dictionary{
		Products:       productsTable,
		ProductsInput:  productsInputStream,
		Shops:          shopsTable,
		ShopsInput:     shopsInputStream,
		Employees:      employeesTable,
		EmployeesInput: employeesInputStream,
		Customers:      customersTable,
		CustomersInput: customersInputStream,
	}

	// Events input stream
	// This stream will be used as source for all further processing
	purchasesStream, err := streams.CreateStream[dtypes.Purchase](ctx, "purchases_stream", shared.StreamSettings{
		SourceTopic: "purchases_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create purchases stream: %w", err)
	}

	pipe.purchases = purchasesStream

	// Bonus invoices stream representing bonus payments for purchases
	// it calculates as 10% of the total dtypes.Purchase amount
	bonusInvoices, err := streams.CreateStreamAsSelect[dtypes.BonusInvoice](
		ctx,
		"bonus_invoices_stream",
		shared.StreamSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("id").As("payment_id"),
				ksql.F("customer_id").As("customer_id"),
				ksql.F("quantity").Mul("price").Mul(0.1).As("amount"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus invoices stream: %w", err)
	}
	pipe.bonusInvoices = bonusInvoices

	// Bonus balances table that aggregates bonus invoices
	// it calculates the total bonus balance for each dtypes.Customer
	bonusBalances, err := tables.CreateTableAsSelect[dtypes.BonusBalance](
		ctx,
		"bonus_balances_table",
		shared.TableSettings{SourceTopic: "bonus_invoices_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("customer_id").As("customer_id"),
				ksql.Sum(ksql.F("amount")).As("balance"),
			).
			From(ksql.Schema("bonus_invoices_stream", ksql.STREAM)).
			GroupBy(ksql.F("customer_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus balances table: %w", err)
	}
	pipe.bonusBalances = bonusBalances

	// Bonus levels table that categorizes customers based on their bonus balance
	// it assigns levels: bronze, silver, gold based on balance thresholds
	bonusLevels, err := tables.CreateTableAsSelect[dtypes.BonusLevel](
		ctx,
		"bonus_levels_table",
		shared.TableSettings{SourceTopic: "bonus_balances_table", Partitions: 1},
		ksql.Select(
			ksql.F("customer_id"),
			ksql.Case(
				ksql.CaseWhen(ksql.F("balance").Less(10_000), "bronze"),
				ksql.CaseWhen(ksql.And(ksql.F("balance").Greater(10_000), ksql.F("balance").Less(100_000)), "silver"),
				ksql.CaseWhen(ksql.F("balance").Greater(100_000), "gold"),
			),
		).EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus levels table: %w", err)
	}
	pipe.bonusLevels = bonusLevels

	// Region analytics table that aggregates sales data by dtypes.Shop region
	// it calculates total sales and dtypes.Purchase count for each dtypes.Shop region
	regionAnalytics, err := tables.CreateTableAsSelect[dtypes.RegionAnalytics](
		ctx,
		"region_analytics_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("shop_id").As("region"),
				ksql.Sum(ksql.F("price").Mul("quantity")).As("total_sales"),
				ksql.Count(ksql.F("id")).As("purchase_count"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)).
			GroupBy(ksql.F("shop_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create region analytics table: %w", err)
	}
	pipe.regionAnalytics = regionAnalytics

	// Seller KPI table that aggregates sales data for each seller
	// it calculates total revenue and total sales for each seller
	sellerKPI, err := tables.CreateTableAsSelect[dtypes.SellerKPI](
		ctx,
		"seller_kpi_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("seller_id"),
				ksql.Count(ksql.F("id")).As("total_sales"),
				ksql.Sum(ksql.F("price").Mul("quantity")).As("total_revenue"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)).
			GroupBy(ksql.F("seller_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create seller kpi table: %w", err)
	}
	pipe.sellerKPI = sellerKPI

	// Seller salary table that calculates seller salaries based on their sales
	// it calculates 5% of the total sales amount for each seller
	sellerSalary, err := tables.CreateTableAsSelect[dtypes.SellerSalary](
		ctx,
		"seller_salary_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("seller_id"),
				ksql.Sum(ksql.F("price").Mul("quantity")).Mul(0.05).As("salary"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)).
			GroupBy(ksql.F("seller_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create seller salary table: %w", err)
	}
	pipe.sellerSalary = sellerSalary

	// Favorite categories table that aggregates dtypes.Customer purchases by category
	// it calculates the most purchased category for each dtypes.Customer
	favoriteCategories, err := tables.CreateTableAsSelect[dtypes.FavoriteCategory](
		ctx,
		"favorite_categories_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("customer_id"),
				ksql.F("product_id").As("category"),
				ksql.Count(ksql.F("id")).As("count"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)).
			GroupBy(ksql.F("customer_id"), ksql.F("product_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create favorite categories table: %w", err)
	}
	pipe.favoriteCategories = favoriteCategories

	// User notifications table that aggregates dtypes.Purchase counts for each dtypes.Customer
	notifications, err := tables.CreateTableAsSelect[dtypes.UserNotification](
		ctx,
		"notifications_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.
			Select(
				ksql.F("customer_id"),
				ksql.Count(ksql.F("id")).As("purchase_count"),
			).
			From(ksql.Schema("purchases_stream", ksql.STREAM)).
			GroupBy(ksql.F("customer_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create notifications table: %w", err)
	}
	pipe.notifications = notifications

	return pipe, nil
}

func dataInit() (*utils.Data, error) {
	data := utils.NewData()

	err := data.LoadCustomers("/data/customers.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load customers: %w", err)
	}
	err = data.LoadShops("/data/shops.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load shops: %w", err)
	}

	err = data.LoadProducts("/data/products.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load products: %w", err)
	}

	err = data.LoadEmployees("/data/employees.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load employees: %w", err)
	}

	// Generate purchases according to existing data (to save relations)
	err = data.GeneratePurchases(100)
	if err != nil {
		return nil, fmt.Errorf("failed to generate purchases: %w", err)
	}

	return data, nil
}

func produceDictionaryData(
	ctx context.Context,
	dict *Dictionary,
	data *utils.Data,
) error {
	// Push each item to the corresponding stream
	for _, product := range data.Products {
		if err := dict.ProductsInput.Insert(ctx, product); err != nil {
			return fmt.Errorf("produce product: %w", err)
		}
	}

	for _, shop := range data.Shops {
		if err := dict.ShopsInput.Insert(ctx, shop); err != nil {
			return fmt.Errorf("produce shop: %w", err)
		}
	}

	for _, employee := range data.Employees {
		if err := dict.EmployeesInput.Insert(ctx, employee); err != nil {
			return fmt.Errorf("produce employee: %w", err)
		}
	}

	for _, customer := range data.Customers {
		if err := dict.CustomersInput.Insert(ctx, customer); err != nil {
			return fmt.Errorf("produce customer: %w", err)
		}
	}

	return nil
}

func producePurchaseEvents(
	ctx context.Context,
	purchaseStream *streams.Stream[dtypes.Purchase],
	purchases []dtypes.Purchase,
) error {
	for _, purchase := range purchases {
		if err := purchaseStream.Insert(ctx, purchase); err != nil {
			return fmt.Errorf("produce purchase event: %w", err)
		}
	}
	return nil

}

func main() {
	ctx := context.Background()
	slog.SetLogLoggerLevel(slog.LevelDebug)

	// Initialize the purchases pipeline
	pipeline, err := NewPipeline(ctx)
	if err != nil {
		fmt.Printf("Error initializing pipeline: %v\n", err)
		return
	}

	// Initialize the data
	data, err := dataInit()
	if err != nil {
		fmt.Printf("Error initializing data: %v\n", err)
		return
	}

	// 1. Initialize dictionary tables by pushing to input streams
	err = produceDictionaryData(ctx, pipeline.dictionary, data)
	if err != nil {
		fmt.Printf("Error producing dictionary data: %v\n", err)
		return
	}

	// 2. Produce purchase events to the main stream
	err = producePurchaseEvents(ctx, pipeline.purchases, data.Purchases)
	if err != nil {
		fmt.Printf("Error producing purchase events: %v\n", err)
		return
	}

	fmt.Println("Pipeline and data initialized successfully.")
}
