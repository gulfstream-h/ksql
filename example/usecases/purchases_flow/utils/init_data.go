package utils

import (
	"encoding/json"
	"fmt"
	"github.com/gulfstream-h/ksql/example/usecases/purchases_flow/dtypes"
	"os"
)

type Data struct {
	Shops     []dtypes.Shop
	Products  []dtypes.Product
	Customers []dtypes.Customer
	Employees []dtypes.Employee
	Purchases []dtypes.Purchase
}

func NewData() *Data {
	return &Data{
		Shops:     []dtypes.Shop{},
		Products:  []dtypes.Product{},
		Customers: []dtypes.Customer{},
		Employees: []dtypes.Employee{},
		Purchases: []dtypes.Purchase{},
	}
}

func (d *Data) LoadShops(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&d.Shops); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func (d *Data) LoadProducts(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&d.Products); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func (d *Data) LoadCustomers(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&d.Customers); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func (d *Data) LoadEmployees(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&d.Employees); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}

func (d *Data) GeneratePurchases(count int) error {
	for i := 0; i < count; i++ {
		// Randomly select a shop
		shop := d.Shops[i%len(d.Shops)]

		// Randomly select an employee from the shop
		var employee dtypes.Employee
		for _, e := range d.Employees {
			if e.ShopID == shop.ID {
				employee = e
				break
			}
		}

		// Randomly select a product
		product := d.Products[i%len(d.Products)]

		// Randomly select a customer
		customer := d.Customers[i%len(d.Customers)]

		// Generate a purchase
		purchase := dtypes.Purchase{
			ID:           fmt.Sprintf("purchase_%d", i),
			CustomerID:   customer.ID,
			SellerID:     employee.ID,
			ProductID:    product.ID,
			ShopID:       shop.ID,
			Quantity:     1 + i%10, // Random quantity between 1 and 10
			Price:        product.Price,
			PurchaseDate: "2023-10-01", // Example date
		}

		// Add the purchase to the list
		d.Purchases = append(d.Purchases, purchase)
	}

	return nil
}
