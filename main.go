package main

import (
	"encoding/json"
	"github.com/labstack/echo/v4"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"interview/internal/consumer"
	"interview/internal/db"
	"log"
	"net/http"
	"time"
)

func main() {
	initConfig()

	dbConfig := db.Config{
		Host:     viper.GetString("database.host"),
		Port:     viper.GetInt("database.port"),
		User:     viper.GetString("database.user"),
		Password: viper.GetString("database.password"),
		DBName:   viper.GetString("database.dbname"),
		SSLMode:  viper.GetString("database.sslmode"),
	}
	db.ConnectToDB(dbConfig)

	done := make(chan bool)
	errChan := make(chan error)

	var cons consumer.Consumer
	source := viper.GetString("source")

	if source == "kafka" {
		cons = consumer.NewKafkaConsumer(
			viper.GetStringSlice("kafka.brokers"),
			viper.GetString("kafka.topic"),
			viper.GetString("kafka.group_id"),
			viper.GetInt("kafka.batch_size"),
		)
	} else {
		cons = consumer.NewCSVConsumer(viper.GetString("csv_file"), 30*time.Minute)
	}

	go readBackground(done, errChan, cons)

	e := echo.New()
	e.GET("/:id", getPromotion)
	e.POST("/promotion", postPromotion)

	go func() {
		if err := e.Start(":1323"); err != nil {
			log.Fatal(err)
		}
	}()

	select {
	case err := <-errChan:
		log.Fatalf("Consumer error: %v", err)
	case <-done:
		log.Println("Application shutdown")
	}
}

func initConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.config/app")
	viper.AddConfigPath("/etc/app")

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
}
func getPromotion(c echo.Context) error {
	id := c.Param("id")
	promotion, err := db.Get(id)
	if err != nil {
		if err.Error() == "record not found" {
			return c.String(http.StatusNotFound, "promotion not found")
		}
		log.Println(err)
		return c.String(http.StatusInternalServerError, "internal server error")
	}
	return c.JSON(http.StatusOK, promotion)
}
func postPromotion(c echo.Context) error {
	var promo db.Promotion
	if err := c.Bind(&promo); err != nil {
		log.Println("error binding JSON data:", err)
		return c.String(http.StatusBadRequest, "invalid request payload")
	}

	writer := kafka.Writer{
		Addr:     kafka.TCP(viper.GetStringSlice("kafka.brokers")[0]),
		Topic:    viper.GetString("kafka.topic"),
		Balancer: &kafka.LeastBytes{},
	}

	promoJSON, err := json.Marshal(promo)
	if err != nil {
		log.Println("error marshaling promotion:", err)
		return c.String(http.StatusInternalServerError, "cannot marshal promotion")
	}

	err = writer.WriteMessages(c.Request().Context(),
		kafka.Message{
			Value: promoJSON,
		},
	)
	if err != nil {
		log.Println("error sending message to Kafka:", err)
		return c.String(http.StatusInternalServerError, "error")
	}

	return c.String(http.StatusOK, "done")
}
func readBackground(done chan bool, errChan chan error, cons consumer.Consumer) {
	for {
		if err := cons.Consume(); err != nil {
			errChan <- err
			return
		}
	}
}
