package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"github.com/gulfstream-h/ksql/tables"
)

type (
	Product struct {
		ID          string `ksql:"id, primary"`
		Name        string `ksql:"name"`
		Description string `ksql:"description"`
		Category    string `ksql:"category"`
	}

	Shop struct {
		ID          string `ksql:"id, primary"`
		Name        string `ksql:"name"`
		Description string `ksql:"description"`
		Region      string `ksql:"region"`
		Address     string `ksql:"address"`
	}

	Customer struct {
		ID      string `ksql:"id, primary"`
		Name    string `ksql:"name"`
		Email   string `ksql:"email"`
		Phone   string `ksql:"phone"`
		Address string `ksql:"address"`
	}

	Employee struct {
		ID       string `ksql:"id, primary"`
		Name     string `ksql:"name"`
		Email    string `ksql:"email"`
		Phone    string `ksql:"phone"`
		ShopID   string `ksql:"shop_id"`
		Position string `ksql:"position"`
	}

	Purchase struct {
		ID           string  `ksql:"id"`
		CustomerID   string  `ksql:"customer_id"`
		SellerID     string  `ksql:"seller_id"`
		ProductID    string  `ksql:"product_id"`
		ShopID       string  `ksql:"shop_id"`
		Quantity     int     `ksql:"quantity"`
		Price        float64 `ksql:"price"`
		PurchaseDate string  `ksql:"purchase_date"`
	}

	BonusInvoice struct {
		Amount     float64 `ksql:"amount"`
		PaymentID  string  `ksql:"payment_id"`
		CustomerID string  `ksql:"customer_id"`
	}

	BonusBalance struct {
		Balance    float64 `ksql:"balance"`
		CustomerID string  `ksql:"customer_id, primary"`
	}

	BonusLevel struct {
		Level      string `ksql:"level"`
		CustomerID string `ksql:"customer_id, primary"`
	}

	RegionAnalytics struct {
		Region        string  `ksql:"region, primary"`
		TotalSales    float64 `ksql:"total_sales"`
		PurchaseCount int     `ksql:"purchase_count"`
	}

	SellerKPI struct {
		SellerID     string  `ksql:"seller_id, primary"`
		TotalRevenue float64 `ksql:"total_revenue"`
		TotalSales   int     `ksql:"total_sales"`
	}

	SellerSalary struct {
		SellerID string  `ksql:"seller_id, primary"`
		Salary   float64 `ksql:"salary"`
	}

	FavoriteCategory struct {
		CustomerID string `ksql:"customer_id, primary"`
		Category   string `ksql:"category, primary"`
		Count      int    `ksql:"count"`
	}

	UserNotification struct {
		CustomerID    string `ksql:"customer_id, primary"`
		PurchaseCount int    `ksql:"purchase_count"`
	}

	Dictionary struct {
		Products  *tables.Table[Product]
		Shops     *tables.Table[Shop]
		Employees *tables.Table[Employee]
		Customers *tables.Table[Customer]
	}

	PurchasesPipeline struct {
		dictionary         *Dictionary
		purchases          *streams.Stream[Purchase]
		bonusInvoices      *streams.Stream[BonusInvoice]
		bonusBalances      *tables.Table[BonusBalance]
		bonusLevels        *tables.Table[BonusLevel]
		regionAnalytics    *tables.Table[RegionAnalytics]
		sellerKPI          *tables.Table[SellerKPI]
		sellerSalary       *tables.Table[SellerSalary]
		favoriteCategories *tables.Table[FavoriteCategory]
		notifications      *tables.Table[UserNotification]
	}
)

const (
	PurchasesTopicName = `purchases_topic`
)

func NewPipeline(ctx context.Context) (*PurchasesPipeline, error) {
	pipe := new(PurchasesPipeline)

	/*
		At first, we should create all necessary tables and streams
		for join it to streams/tables in the pipeline
	*/

	// Shops dictionary
	shopsTable, err := tables.CreateTable[Shop](ctx, "shops_table", shared.TableSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create shops table: %w", err)
	}

	// Products dictionary
	productsTable, err := tables.CreateTable[Product](ctx, "products_table", shared.TableSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create products table: %w", err)
	}

	// Employees dictionary
	employeesTable, err := tables.CreateTable[Employee](ctx, "employees_table", shared.TableSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create employees table: %w", err)
	}

	// Customers dictionary
	customersTable, err := tables.CreateTable[Customer](ctx, "customers_table", shared.TableSettings{Partitions: 1})
	if err != nil {
		return nil, fmt.Errorf("create customers table: %w", err)
	}

	pipe.dictionary = &Dictionary{
		Products:  productsTable,
		Shops:     shopsTable,
		Employees: employeesTable,
		Customers: customersTable,
	}

	// Events input stream
	// This stream will be used as source for all further processing
	purchasesStream, err := streams.CreateStream[Purchase](ctx, "purchases_stream", shared.StreamSettings{
		SourceTopic: "purchases_topic",
		Partitions:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("create purchases stream: %w", err)
	}

	pipe.purchases = purchasesStream

	// Bonus invoices stream representing bonus payments for purchases
	// it calculates as 10% of the total purchase amount
	bonusInvoices, err := streams.CreateStreamAsSelect[BonusInvoice](
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
	// it calculates the total bonus balance for each customer
	bonusBalances, err := tables.CreateTableAsSelect[BonusBalance](
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
	bonusLevels, err := tables.CreateTableAsSelect[BonusLevel](
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

	// Region analytics table that aggregates sales data by shop region
	// it calculates total sales and purchase count for each shop region
	regionAnalytics, err := tables.CreateTableAsSelect[RegionAnalytics](
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
	sellerKPI, err := tables.CreateTableAsSelect[SellerKPI](
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
	sellerSalary, err := tables.CreateTableAsSelect[SellerSalary](
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

	// Favorite categories table that aggregates customer purchases by category
	// it calculates the most purchased category for each customer
	favoriteCategories, err := tables.CreateTableAsSelect[FavoriteCategory](
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

	// User notifications table that aggregates purchase counts for each customer
	notifications, err := tables.CreateTableAsSelect[UserNotification](
		ctx,
		"notifications_table",
		shared.TableSettings{SourceTopic: "purchases_stream", Partitions: 1},
		ksql.Select(
			ksql.F("customer_id"),
			ksql.Count(ksql.F("id")).As("purchase_count"),
		).From(ksql.Schema("purchases_stream", ksql.STREAM)).GroupBy(ksql.F("customer_id")),
	)
	if err != nil {
		return nil, fmt.Errorf("create notifications table: %w", err)
	}
	pipe.notifications = notifications

	return pipe, nil
}
