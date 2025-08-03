package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/config"
	"github.com/gulfstream-h/ksql/example/usecases/purchases_flow/dtypes"
	"github.com/gulfstream-h/ksql/example/usecases/purchases_flow/utils"
	"github.com/gulfstream-h/ksql/kinds"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"github.com/gulfstream-h/ksql/tables"
	"log"
	"log/slog"
	"os"
)

const (
	// Stream names

	ProductsStreamName      = "products_stream"
	ShopsStreamName         = "shops_stream"
	EmployeesStreamName     = "employees_stream"
	CustomersStreamName     = "customers_stream"
	PurchasesStreamName     = "purchases_stream"
	BonusInvoicesStreamName = "bonus_invoices_stream"

	// Table names

	ProductsTableName           = "products_table"
	ShopsTableName              = "shops_table"
	EmployeesTableName          = "employees_table"
	CustomersTableName          = "customers_table"
	BonusBalancesTableName      = "bonus_balances_table"
	BonusLevelsTableName        = "bonus_levels_table"
	RegionAnalyticsTableName    = "region_analytics_table"
	SellerKPITableName          = "seller_kpi_table"
	SellerSalaryTableName       = "seller_salary_table"
	FavoriteCategoriesTableName = "favorite_categories_table"
	NotificationsTableName      = "notifications_table"

	// Topics

	PurchasesTopic = "purchases_topic"
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
	productsInputStream, err := streams.CreateStream[dtypes.Product](ctx, ProductsStreamName, shared.StreamSettings{
		Name:        ProductsStreamName,
		SourceTopic: "products_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create products stream: %w", err)
	}

	// Shops input stream
	shopsInputStream, err := streams.CreateStream[dtypes.Shop](ctx, ShopsStreamName, shared.StreamSettings{
		Name:        ShopsStreamName,
		SourceTopic: "shops_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create shops stream: %w", err)
	}

	// Employees input stream
	employeesInputStream, err := streams.CreateStream[dtypes.Employee](ctx, EmployeesStreamName, shared.StreamSettings{
		Name:        EmployeesStreamName,
		SourceTopic: "employees_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create employees stream: %w", err)
	}

	// Customers input stream
	customersInputStream, err := streams.CreateStream[dtypes.Customer](ctx, CustomersStreamName, shared.StreamSettings{
		Name:        CustomersStreamName,
		SourceTopic: "customers_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create customers stream: %w", err)
	}

	// Shops dictionary
	shopsTable, err := tables.CreateTable[dtypes.Shop](ctx, ShopsTableName, shared.TableSettings{
		Name:        ShopsTableName,
		SourceTopic: ShopsStreamName,
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create shops table: %w", err)
	}

	// Products dictionary
	productsTable, err := tables.CreateTable[dtypes.Product](ctx, ProductsTableName, shared.TableSettings{
		Name:        ProductsTableName,
		SourceTopic: ProductsStreamName,
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create products table: %w", err)
	}

	// Employees dictionary
	employeesTable, err := tables.CreateTable[dtypes.Employee](ctx, EmployeesTableName, shared.TableSettings{
		Name:        EmployeesTableName,
		SourceTopic: EmployeesStreamName,
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create employees table: %w", err)
	}

	// Customers dictionary
	customersTable, err := tables.CreateTable[dtypes.Customer](ctx, CustomersTableName, shared.TableSettings{
		Name:        CustomersTableName,
		SourceTopic: CustomersStreamName,
		Partitions:  1,
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
	purchasesStream, err := streams.CreateStream[dtypes.Purchase](ctx, PurchasesStreamName, shared.StreamSettings{
		Name:        PurchasesStreamName,
		SourceTopic: PurchasesTopic,
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
		BonusInvoicesStreamName,
		shared.StreamSettings{
			Name:        BonusInvoicesStreamName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("id").As("payment_id"),
			ksql.F("customer_id").As("customer_id"),
			ksql.Mul(ksql.Mul(ksql.F("quantity"), ksql.F("price")), 0.1).As("amount"),
		).From(ksql.Schema(PurchasesStreamName, ksql.STREAM)),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus invoices stream: %w", err)
	}
	pipe.bonusInvoices = bonusInvoices

	// Bonus balances table that aggregates bonus invoices
	// it calculates the total bonus balance for each dtypes.Customer
	bonusBalances, err := tables.CreateTableAsSelect[dtypes.BonusBalance](
		ctx,
		BonusBalancesTableName,
		shared.TableSettings{
			Name:        BonusBalancesTableName,
			SourceTopic: BonusInvoicesStreamName,
			Partitions:  1,
		},
		ksql.
			Select(
				ksql.F("customer_id").As("customer_id"),
				ksql.Sum(ksql.F("amount")).As("balance"),
			).
			From(ksql.Schema(BonusInvoicesStreamName, ksql.STREAM)).
			GroupBy(ksql.F("customer_id")).
			EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus balances table: %w", err)
	}
	pipe.bonusBalances = bonusBalances

	// Bonus levels table that categorizes customers based on their bonus balance
	// it assigns levels: bronze, silver, gold based on balance thresholds
	bonusLevels, err := tables.CreateTableAsSelect[dtypes.BonusLevel](
		ctx,
		BonusLevelsTableName,
		shared.TableSettings{
			Name:        BonusLevelsTableName,
			SourceTopic: BonusBalancesTableName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("customer_id"),
			ksql.Case(
				"level",
				ksql.CaseWhen(ksql.F("balance").Less(10_000), "bronze"),
				ksql.CaseWhen(ksql.And(ksql.F("balance").Greater(10_000), ksql.F("balance").Less(100_000)), "silver"),
				ksql.CaseWhen(ksql.F("balance").Greater(100_000), "gold"),
			),
		).
			From(ksql.Schema(BonusBalancesTableName, ksql.TABLE)).
			EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create bonus levels table: %w", err)
	}
	pipe.bonusLevels = bonusLevels

	// Region analytics table that aggregates sales data by dtypes.Shop region
	// it calculates total sales and dtypes.Purchase count for each dtypes.Shop region
	regionAnalytics, err := tables.CreateTableAsSelect[dtypes.RegionAnalytics](
		ctx,
		RegionAnalyticsTableName,
		shared.TableSettings{
			Name:        RegionAnalyticsTableName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("shop_id").As("region"),
			ksql.Sum(ksql.Mul(ksql.F("price"), ksql.F("quantity"))).As("total_sales"),
			ksql.Count(ksql.F("id")).As("purchase_count"),
		).
			Windowed(
				ksql.NewTumblingWindow(
					ksql.TimeUnit{
						Val:  1,
						Unit: ksql.Days,
					},
				),
			).
			From(ksql.Schema(PurchasesStreamName, ksql.STREAM)).
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
		SellerKPITableName,
		shared.TableSettings{
			Name:        SellerKPITableName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("seller_id").As("seller_id"),
			ksql.Count(ksql.F("id")).As("total_sales"),
			ksql.Sum(ksql.Mul(ksql.F("price"), ksql.F("quantity"))).As("total_revenue"),
		).
			From(ksql.Schema(PurchasesStreamName, ksql.STREAM)).
			GroupBy(ksql.F("seller_id")).
			EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create seller KPI table: %w", err)
	}
	pipe.sellerKPI = sellerKPI

	// Seller salary table that calculates seller salaries based on their sales
	// it calculates 5% of the total sales amount for each seller
	sellerSalary, err := tables.CreateTableAsSelect[dtypes.SellerSalary](
		ctx,
		SellerSalaryTableName,
		shared.TableSettings{
			Name:        SellerSalaryTableName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("seller_id").As("seller_id"),
			ksql.Mul(
				ksql.Sum(ksql.Mul(ksql.F("price"), ksql.F("quantity"))),
				0.05,
			).As("salary"),
		).
			From(ksql.Schema(PurchasesStreamName, ksql.STREAM)).
			GroupBy(ksql.F("seller_id")).
			EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create seller salary table: %w", err)
	}
	pipe.sellerSalary = sellerSalary

	// Favorite categories table that aggregates dtypes.Customer purchases by category
	// it calculates the most purchased category for each dtypes.Customer
	favoriteCategories, err := tables.CreateTableAsSelect[dtypes.FavoriteCategory](
		ctx,
		FavoriteCategoriesTableName,
		shared.TableSettings{
			Name:        FavoriteCategoriesTableName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
			KeyFormat:   kinds.JSON,
		},
		ksql.
			Select(
				ksql.F("customer_id").As("customer_id"),
				ksql.F("product_id").As("category"),
				ksql.Count(ksql.F("id")).As("count"),
			).
			From(ksql.Schema(PurchasesStreamName, ksql.STREAM)).
			GroupBy(ksql.F("customer_id"), ksql.F("product_id")).
			EmitChanges(),
	)
	if err != nil {
		return nil, fmt.Errorf("create favorite categories table: %w", err)
	}
	pipe.favoriteCategories = favoriteCategories

	// User notifications table that aggregates dtypes.Purchase counts for each dtypes.Customer
	notifications, err := tables.CreateTableAsSelect[dtypes.UserNotification](
		ctx,
		NotificationsTableName,
		shared.TableSettings{
			Name:        NotificationsTableName,
			SourceTopic: PurchasesStreamName,
			Partitions:  1,
		},
		ksql.
			Select(
				ksql.F("customer_id"),
				ksql.Count(ksql.F("id")).As("purchase_count"),
			).
			From(ksql.Schema(PurchasesStreamName, ksql.STREAM)).
			Windowed(
				ksql.NewTumblingWindow(
					ksql.TimeUnit{Val: 10, Unit: ksql.Minutes},
				),
			).
			GroupBy(ksql.F("customer_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create notifications table: %w", err)
	}
	pipe.notifications = notifications

	return pipe, nil
}

func Cleanup(ctx context.Context) {
	slog.Info("Starting to drop streams and tables...")

	// Drop dependent tables first (most downstream)
	slog.Info("Dropping dependent tables...")

	if err := tables.Drop(ctx, NotificationsTableName); err != nil {
		slog.Error("Failed to drop notifications table", "err", err)
	} else {
		slog.Info("Dropped notifications table")
	}

	if err := tables.Drop(ctx, FavoriteCategoriesTableName); err != nil {
		slog.Error("Failed to drop favorite categories table", "err", err)
	} else {
		slog.Info("Dropped favorite categories table")
	}

	if err := tables.Drop(ctx, SellerSalaryTableName); err != nil {
		slog.Error("Failed to drop seller salary table", "err", err)
	} else {
		slog.Info("Dropped seller salary table")
	}

	if err := tables.Drop(ctx, SellerKPITableName); err != nil {
		slog.Error("Failed to drop seller KPI table", "err", err)
	} else {
		slog.Info("Dropped seller KPI table")
	}

	if err := tables.Drop(ctx, RegionAnalyticsTableName); err != nil {
		slog.Error("Failed to drop region analytics table", "err", err)
	} else {
		slog.Info("Dropped region analytics table")
	}

	if err := tables.Drop(ctx, BonusLevelsTableName); err != nil {
		slog.Error("Failed to drop bonus levels table", "err", err)
	} else {
		slog.Info("Dropped bonus levels table")
	}

	if err := tables.Drop(ctx, BonusBalancesTableName); err != nil {
		slog.Error("Failed to drop bonus balances table", "err", err)
	} else {
		slog.Info("Dropped bonus balances table")
	}

	// Drop intermediate streams (used as input for tables above)
	slog.Info("Dropping intermediate streams...")

	if err := streams.Drop(ctx, BonusInvoicesStreamName); err != nil {
		slog.Error("Failed to drop bonus invoices stream", "err", err)
	} else {
		slog.Info("Dropped bonus invoices stream")
	}

	if err := streams.Drop(ctx, PurchasesStreamName); err != nil {
		slog.Error("Failed to drop purchases stream", "err", err)
	} else {
		slog.Info("Dropped purchases stream")
	}

	// Drop dictionary tables
	slog.Info("Dropping dictionary tables...")

	if err := tables.Drop(ctx, ProductsTableName); err != nil {
		slog.Error("Failed to drop products table", "err", err)
	} else {
		slog.Info("Dropped products table")
	}

	if err := tables.Drop(ctx, ShopsTableName); err != nil {
		slog.Error("Failed to drop shops table", "err", err)
	} else {
		slog.Info("Dropped shops table")
	}

	if err := tables.Drop(ctx, EmployeesTableName); err != nil {
		slog.Error("Failed to drop employees table", "err", err)
	} else {
		slog.Info("Dropped employees table")
	}

	if err := tables.Drop(ctx, CustomersTableName); err != nil {
		slog.Error("Failed to drop customers table", "err", err)
	} else {
		slog.Info("Dropped customers table")
	}

	// Drop input streams (raw data sources)
	slog.Info("Dropping input streams...")

	if err := streams.Drop(ctx, ProductsStreamName); err != nil {
		slog.Error("Failed to drop products input stream", "err", err)
	} else {
		slog.Info("Dropped products input stream")
	}

	if err := streams.Drop(ctx, ShopsStreamName); err != nil {
		slog.Error("Failed to drop shops input stream", "err", err)
	} else {
		slog.Info("Dropped shops input stream")
	}

	if err := streams.Drop(ctx, EmployeesStreamName); err != nil {
		slog.Error("Failed to drop employees input stream", "err", err)
	} else {
		slog.Info("Dropped employees input stream")
	}

	if err := streams.Drop(ctx, CustomersStreamName); err != nil {
		slog.Error("Failed to drop customers input stream", "err", err)
	} else {
		slog.Info("Dropped customers input stream")
	}

	slog.Info("All streams and tables dropped (or attempted).")
}

func dataInit() (*utils.Data, error) {
	data := utils.NewData()

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get working directory: %v", err)
	}
	log.Printf("Working directory: %s", wd)

	err = data.LoadCustomers("example/usecases/purchases_flow/data/customers.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load customers: %w", err)
	}
	err = data.LoadShops("example/usecases/purchases_flow/data/shops.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load shops: %w", err)
	}

	err = data.LoadProducts("example/usecases/purchases_flow/data/products.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load products: %w", err)
	}

	err = data.LoadEmployees("example/usecases/purchases_flow/data/employees.json")
	if err != nil {
		return nil, fmt.Errorf("failed to load employees: %w", err)
	}

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

	err := config.
		New("http://localhost:8088", 600, true).
		Configure(ctx)

	if err != nil {
		fmt.Printf("Failed to configure ksql: %v\n", err)
		return
	}

	// special cleanup function to drop all streams and tables using in this example
	// that was created in previous run
	Cleanup(ctx)
	Cleanup(ctx)

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
