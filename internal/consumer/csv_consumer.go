package consumer

import (
	"encoding/csv"
	"fmt"
	"interview/internal/db"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type csvConsumer struct {
	FileName string
	Interval time.Duration
}

func NewCSVConsumer(fileName string, interval time.Duration) Consumer {
	return csvConsumer{
		FileName: fileName,
		Interval: interval,
	}
}
func (c csvConsumer) Consume() error {
	ticker := time.NewTicker(c.Interval)
	for {
		select {
		case <-ticker.C:
			var promotions []db.Promotion
			records, err := readCsv(c.FileName)
			if err != nil {
				return err
			}
			for _, line := range records[1:] {
				price, err := strconv.ParseFloat(line[1], 64)
				date, err := time.Parse("2006-01-02", strings.Split(line[2], " ")[0])
				if err != nil {
					log.Println(fmt.Sprintf("cannot parse line: %s with err: %v", line, err))
					continue
				}
				promotions = append(promotions, db.Promotion{
					ID:             line[0],
					Price:          price,
					ExpirationDate: &date,
				})
			}
			err = db.BulkInsert(promotions)
			if err != nil {
				return err
			}
		}
	}
}

func readCsv(fileName string) ([][]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error while reading the file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading records: %v", err)
	}

	return records, nil
}
