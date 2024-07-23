package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"interview/internal/db"
	"log"
	"strings"
	"time"
)

type kafkaConsumer struct {
	Brokers   []string
	Topic     string
	GroupID   string
	BatchSize int
	Reader    *kafka.Reader
}

func NewKafkaConsumer(brokers []string, topic, groupID string, batchSize int) Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})
	return kafkaConsumer{
		Brokers:   brokers,
		Topic:     topic,
		GroupID:   groupID,
		BatchSize: batchSize,
		Reader:    reader,
	}
}

func (k kafkaConsumer) Consume() error {
	defer k.Reader.Close()

	for {
		messageBatch := make([]kafka.Message, 0, k.BatchSize)
		for len(messageBatch) < k.BatchSize {
			message, err := k.Reader.ReadMessage(context.Background())
			if err != nil {
				return fmt.Errorf("error reading message from Kafka: %v", err)
			}
			messageBatch = append(messageBatch, message)
		}

		var promotions []db.Promotion
		for _, message := range messageBatch {
			var promo db.Promotion
			err := json.Unmarshal(message.Value, &promo)
			if err != nil {
				log.Println("error unmarshaling message:", err)
				continue
			}

			price := promo.Price
			date, err := time.Parse("2006-01-02", strings.Split(promo.ExpirationDate.String(), " ")[0])
			if err != nil {
				log.Println(fmt.Sprintf("cannot parse date: %s with err: %v", promo.ExpirationDate, err))
				continue
			}

			dbPromotion := db.Promotion{
				ID:             promo.ID,
				Price:          price,
				ExpirationDate: &date,
			}

			promotions = append(promotions, dbPromotion)
		}

		err := db.BulkInsert(promotions)
		if err != nil {
			log.Println("error inserting promotions:", err)
			return err
		}
	}
}
