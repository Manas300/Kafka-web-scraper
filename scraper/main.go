package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "strings"
    "time"

    "github.com/PuerkitoBio/goquery"
    "github.com/segmentio/kafka-go"
)

func main() {
    if len(os.Args) < 2 {
        log.Fatal("Usage: scraper <URL>")
    }

    url := os.Args[1]
    log.Println(" Scraping URL:", url)

    req, _ := http.NewRequest("GET", url, nil)
    req.Header.Set("User-Agent", "Mozilla/5.0")

    client := &http.Client{}
    res, err := client.Do(req)
    if err != nil {
        log.Fatalf(" HTTP request failed: %v", err)
    }
    defer res.Body.Close()

    if res.StatusCode != 200 {
        log.Fatalf(" Bad HTTP status: %d", res.StatusCode)
    }

    doc, err := goquery.NewDocumentFromReader(res.Body)
    if err != nil {
        log.Fatalf(" Failed to parse HTML: %v", err)
    }

    // Kafka setup
    topic := "scraped_table_data"
    brokers := []string{"localhost:9092"}
    log.Println("Connecting to Kafka brokers:", brokers)

    // Step 1: Connect to broker directly
    conn, err := kafka.Dial("tcp", brokers[0])
    if err != nil {
        log.Fatalf(" Failed to dial Kafka broker: %v", err)
    }
    defer conn.Close()

    // Step 2: Find controller to create topic
    controller, err := conn.Controller()
    if err != nil {
        log.Fatalf(" Failed to get controller: %v", err)
    }

    controllerAddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
    controllerConn, err := kafka.Dial("tcp", controllerAddr)
    if err != nil {
        log.Fatalf(" Failed to dial controller broker at %s: %v", controllerAddr, err)
    }
    defer controllerConn.Close()

    err = controllerConn.CreateTopics(kafka.TopicConfig{
        Topic:             topic,
        NumPartitions:     1,
        ReplicationFactor: 1,
    })
    if err != nil && !strings.Contains(err.Error(), "already exists") {
        log.Fatalf(" Failed to create topic: %v", err)
    }

    // Step 3: Create writer
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers:  brokers,
        Topic:    topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer writer.Close()

    // Step 4: Health check write
    log.Println("Sending Kafka health check...")
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    err = writer.WriteMessages(ctx, kafka.Message{
        Key:   []byte("health-check"),
        Value: []byte("test"),
    })
    if err != nil {
        log.Fatalf(" Kafka write failed: %v", err)
    }

    log.Println("Kafka is reachable")

    // Step 5: Scrape and publish table
    doc.Find("table tbody tr").Each(func(i int, s *goquery.Selection) {
        var row []string
        s.Find("td").Each(func(_ int, td *goquery.Selection) {
            row = append(row, strings.TrimSpace(td.Text()))
        })

        if len(row) > 0 {
            jsonRow, err := json.Marshal(row)
            if err != nil {
                log.Printf(" JSON marshal error at row %d: %v", i, err)
                return
            }

            err = writer.WriteMessages(context.Background(), kafka.Message{
                Key:   []byte(fmt.Sprintf("row-%d", i)),
                Value: jsonRow,
            })
            if err != nil {
                log.Printf(" Failed to publish row-%d: %v", i, err)
            } else {
                log.Printf(" Published row-%d", i)
            }
        }
    })

    log.Println(" Scraping and publishing completed.")
}

