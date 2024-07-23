package db

import (
	"time"
)

type Promotion struct {
	ID             string     `gorm:"primaryKey" ;json:"ID"`
	Price          float64    `json:"price"`
	ExpirationDate *time.Time `json:"expiration_date"`
}
