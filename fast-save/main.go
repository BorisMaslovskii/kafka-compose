package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v4"
	kafka "github.com/segmentio/kafka-go"
)

func main() {
	urlExample := "postgres://postgres:pgpass@localhost:5432?search_path=kafka"
	conn, err := pgx.Connect(context.Background(), urlExample)
	if err != nil {
		log.Print("Unable to connect to database: ", err)
		os.Exit(1)
	}
	defer func(conn *pgx.Conn, ctx context.Context) {
		err := conn.Close(ctx)
		if err != nil {
			log.Print("can't close db connection")
		}
	}(conn, context.Background())

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{"localhost:9092"},
		Topic:          "fast",
		GroupID:        "fastread-group",
		CommitInterval: 10 * time.Second,
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(r *kafka.Reader) {
		<-c
		cleanup(r)
		os.Exit(1)
	}(r)

	batch := &pgx.Batch{}

	//count := 0
	timer := time.NewTicker(1 * time.Second)
	go func() {
		for {
			<-timer.C
			//log.Print(count)

			br := conn.SendBatch(context.Background(), batch)
			err := br.Close()
			if err != nil {
				os.Exit(1)
			}
			batch = &pgx.Batch{}

			var rowsCount int64
			err = conn.QueryRow(context.Background(), "select count(*) from messages").Scan(&rowsCount)
			if err != nil {
				log.Printf("QueryRow failed: %v\n", err)
				os.Exit(1)
			}
			log.Print("table count: ", rowsCount)
		}
	}()

	for {
		message, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}

		batch.Queue("insert into messages(i, message) values($1, $2)", string(message.Key), string(message.Value))
	}

}

func cleanup(r *kafka.Reader) {
	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	log.Println("reader closed")
}
