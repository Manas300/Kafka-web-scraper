package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "strings"
    

    _ "github.com/lib/pq"
    "github.com/segmentio/kafka-go"
)

func main() {
    // Configurable for local testing
    kafkaBroker := os.Getenv("KAFKA_BROKER")
    if kafkaBroker == "" {
        kafkaBroker = "localhost:9092"
    }

    kafkaTopic := os.Getenv("KAFKA_TOPIC")
    if kafkaTopic == "" {
        kafkaTopic = "scraped_table_data"
    }

    pgDSN := os.Getenv("POSTGRES_DSN")
    if pgDSN == "" {
        pgDSN = "postgres://kafkauser:kafkapass@localhost:5432/kafkadb?sslmode=disable"
    }

    log.Println(" Connecting to PostgreSQL...")
    db, err := sql.Open("postgres", pgDSN)
    if err != nil {
        log.Fatalf(" Failed to connect to Postgres: %v", err)
    }
    defer db.Close()

    // Auto-create table
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS scraped_data (
        id SERIAL PRIMARY KEY,
        data TEXT[]
    )`)
    if err != nil {
        log.Fatalf(" Failed to create table: %v", err)
    }

    log.Println(" Connected to Postgres. Starting Kafka consumer...")

    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers: []string{kafkaBroker},
        Topic:   kafkaTopic,
        GroupID: "postgres-writer-group",
    })
    defer r.Close()

    for {
        msg, err := r.ReadMessage(context.Background())
        if err != nil {
            log.Printf(" Kafka read error: %v", err)
            continue
        }

        var row []string
        if err := json.Unmarshal(msg.Value, &row); err != nil {
            log.Printf(" JSON unmarshal failed: %v", err)
            continue
        }

        log.Printf(" Inserting row: %v", row)

        _, err = db.Exec(`INSERT INTO scraped_data (data) VALUES ($1)`, pqArray(row))
        if err != nil {
            log.Printf(" DB insert failed: %v", err)
        } else {
            log.Println(" Inserted row")
        }
    }
}

// Helper to convert Go string slice to Postgres TEXT[]
func pqArray(strs []string) interface{} {
    return fmt.Sprintf("{%s}", strings.Join(strs, ","))
}

