package db

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

var db *gorm.DB

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

func ConnectToDB(config Config) {
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		config.User, config.Password, config.Host, config.Port, config.DBName, config.SSLMode)

	// Connect to database
	database, err := gorm.Open(postgres.Open(connStr), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	db = database

	// Automatically migrate the schema
	err = db.AutoMigrate(&Promotion{})
	if err != nil {
		log.Fatal(err)
	}
}

func BulkInsert(unsavedRows []Promotion) error {
	return db.CreateInBatches(&unsavedRows, 100).Error
}

func Get(id string) (Promotion, error) {
	var promotion Promotion
	err := db.First(&promotion, "id = ?", id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return promotion, fmt.Errorf("record not found")
		}
		return promotion, err
	}
	return promotion, nil
}
