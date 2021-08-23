package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v4"
	kafka "github.com/segmentio/kafka-go"
)

func main() {

	// urlExample := "postgres://username:password@localhost:5432/database_name"
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

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

	count := 0
	timer := time.NewTicker(1 * time.Second)
	go func() {
		for {
			<-timer.C
			log.Print(count)
		}
	}()
	for {
		_, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		count++
		//log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

}

func cleanup(r *kafka.Reader) {
	log.Println("writer closed")

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}

	log.Println("reader closed")
}
