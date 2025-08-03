package dtypes

type Product struct {
	ID          string  `ksql:"id, primary"`
	Name        string  `ksql:"name"`
	Price       float64 `ksql:"price"`
	Description string  `ksql:"description"`
	Category    string  `ksql:"category"`
}

type Shop struct {
	ID          string `ksql:"id, primary"`
	Name        string `ksql:"name"`
	Description string `ksql:"description"`
	Region      string `ksql:"region"`
	Address     string `ksql:"address"`
}

type Customer struct {
	ID      string `ksql:"id, primary"`
	Name    string `ksql:"name"`
	Email   string `ksql:"email"`
	Phone   string `ksql:"phone"`
	Address string `ksql:"address"`
}

type Employee struct {
	ID       string `ksql:"id, primary"`
	Name     string `ksql:"name"`
	Email    string `ksql:"email"`
	Phone    string `ksql:"phone"`
	ShopID   string `ksql:"shop_id"`
	Position string `ksql:"position"`
}

type Purchase struct {
	ID           string  `ksql:"id"`
	CustomerID   string  `ksql:"customer_id"`
	SellerID     string  `ksql:"seller_id"`
	ProductID    string  `ksql:"product_id"`
	ShopID       string  `ksql:"shop_id"`
	Quantity     int     `ksql:"quantity"`
	Price        float64 `ksql:"price"`
	PurchaseDate string  `ksql:"purchase_date"`
}

type BonusInvoice struct {
	Amount     float64 `ksql:"amount"`
	PaymentID  string  `ksql:"payment_id"`
	CustomerID string  `ksql:"customer_id"`
}

type BonusBalance struct {
	Balance    float64 `ksql:"balance"`
	CustomerID string  `ksql:"customer_id, primary"`
}

type BonusLevel struct {
	Level      string `ksql:"level"`
	CustomerID string `ksql:"customer_id, primary"`
}

type RegionAnalytics struct {
	Region        string  `ksql:"region, primary"`
	TotalSales    float64 `ksql:"total_sales"`
	PurchaseCount int     `ksql:"purchase_count"`
}

type SellerKPI struct {
	SellerID     string  `ksql:"seller_id, primary"`
	TotalRevenue float64 `ksql:"total_revenue"`
	TotalSales   int     `ksql:"total_sales"`
}

type SellerSalary struct {
	SellerID string  `ksql:"seller_id, primary"`
	Salary   float64 `ksql:"salary"`
}

type FavoriteCategory struct {
	CustomerID string `ksql:"customer_id, primary"`
	Category   string `ksql:"category"`
	Count      int    `ksql:"count"`
}

type UserNotification struct {
	CustomerID    string `ksql:"customer_id, primary"`
	PurchaseCount int    `ksql:"purchase_count"`
}
