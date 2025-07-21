package main

import (
	"context"
	"fmt"
	"github.com/gulfstream-h/ksql/ksql"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"github.com/gulfstream-h/ksql/tables"
)

const (
	purchasesTopicName  = `purchases_topic`
	purchasesStreamName = `purchases_stream`

	bonusBalancesTableName = `bonus_balances_table`
	bonusLevelsTableName   = `bonus_levels_table`

	bonusInvoicesStreamName = `bonus_invoices_stream`
)

type (
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

	Shop struct {
		ID     string `ksql:"id, primary"`
		Region string `ksql:"region"`
		Name   string `ksql:"name"`
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

	PurchasesPipeline struct {
		purchases     *streams.Stream[Purchase]
		bonusInvoices *streams.Stream[BonusInvoice]
		bonusBalances *tables.Table[BonusBalance]
		bonusLevels   *tables.Table[BonusLevel]
	}
)

func NewPipeline(
	ctx context.Context,
) (*PurchasesPipeline, error) {
	pipe := new(PurchasesPipeline)

	purchasesStream, err := streams.CreateStream[Purchase](
		ctx,
		purchasesStreamName,
		shared.StreamSettings{
			SourceTopic: purchasesTopicName,
			Partitions:  1,
		},
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create purchases stream: %w", err)
	}

	pipe.purchases = purchasesStream

	bonusInvoicesStream, err := streams.CreateStreamAsSelect[BonusInvoice](
		ctx,
		bonusInvoicesStreamName,
		shared.StreamSettings{
			SourceTopic: purchasesStreamName,
			Partitions:  1,
		},
		ksql.
			Select(
				ksql.F("id").As("payment_id"),
				ksql.F("customer_id").As("customer_id"),
				ksql.F("quantity").Mul("price").Mul(0.1).As("amount"),
			).
			From(ksql.Schema(purchasesStreamName, ksql.STREAM)),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create bonus invoices stream: %w", err)
	}

	pipe.bonusInvoices = bonusInvoicesStream

	bonusBalancesTable, err := tables.CreateTableAsSelect[BonusBalance](
		ctx,
		bonusBalancesTableName,
		shared.TableSettings{
			SourceTopic: bonusInvoicesStreamName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("customer_id"),
			ksql.Sum(ksql.F("amount")).As("balance"),
		).
			From(ksql.Schema(bonusInvoicesStreamName, ksql.STREAM)).
			GroupBy(ksql.F("customer_id")),
	)

	pipe.bonusBalances = bonusBalancesTable

	bonusLevelsTable, err := tables.CreateTableAsSelect[BonusLevel](
		ctx,
		bonusLevelsTableName,
		shared.TableSettings{
			SourceTopic: bonusBalancesTableName,
			Partitions:  1,
		},
		ksql.Select(
			ksql.F("customer_id"),
			ksql.Case(
				ksql.CaseWhen(
					ksql.F("balance").Less(10_000),
					"bronze",
				),
				ksql.CaseWhen(
					ksql.And(
						ksql.F("balance").Greater(10_000),
						ksql.F("balance").Less(100_000),
					),
					"silver",
				),
				ksql.CaseWhen(
					ksql.F("balance").Greater(100_000),
					"gold",
				),
			),
		).EmitChanges(),
	)

	if err != nil {
		return nil, fmt.Errorf("bonus level table init: %w", err)
	}
	pipe.bonusLevels = bonusLevelsTable

	return pipe, nil
}
