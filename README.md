# Kafka Web Scraper

This project is a lightweight ETL pipeline built using Go, Kafka, and PostgreSQL. It scrapes tabular data from a public website, publishes each row to a Kafka topic, and a Kafka consumer reads and stores the data into a PostgreSQL database.

## Tech Stack

- Go 1.22
- Kafka (via `confluentinc/cp-kafka`)
- PostgreSQL
- Kafka UI (via `provectuslabs/kafka-ui`)
- Docker and Docker Compose

## Project Structure

```
Kafka-web-scraper/
│
├── docker-compose.yml
│
├── scraper/
│   ├── Dockerfile
│   ├── main.go
│   ├── go.mod
│   └── go.sum
│
└── writer/
    ├── main.go
    ├── go.mod
    └── go.sum
```

## Prerequisites

- Go (>= 1.22)
- Docker
- Git

## Setup Instructions

### 1. Clone the repository

```bash
git clone git@github.com:Manas300/Kafka-web-scraper.git
cd Kafka-web-scraper
```

### 2. Start the environment using Docker

```bash
docker-compose up --build -d
```

This will start Kafka, Zookeeper, Kafka UI, and PostgreSQL containers.

### 3. Run the Web Scraper

```bash
cd scraper
go run main.go https://www.worldometers.info/world-population/population-by-country/
```

This will scrape the data and publish each row to the Kafka topic.

### 4. Run the Kafka Consumer

```bash
cd ../writer
go run main.go
```

This will consume messages from Kafka and insert them into PostgreSQL.

## Accessing Services

- Kafka UI: [http://localhost:8080](http://localhost:8080)
- PostgreSQL:
  - Host: `localhost`
  - Port: `5432`
  - User: `postgres`
  - Password: `postgres`

## Checking the Data in PostgreSQL

To access the database via Docker:

```bash
docker exec -it postgres psql -U postgres
```

Then run:

```sql
SELECT * FROM scraped_data;
```

## Resetting the Table

To delete all entries from the table:

```sql
DELETE FROM scraped_data;
```

## Notes

- This implementation assumes a consistent table structure in the target website.
- Rows with missing values may result in insert errors unless handled in preprocessing.
- The project uses the `segmentio/kafka-go` library for Kafka interaction.

## License

MIT License © 2025 Manas Singh
