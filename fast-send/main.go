package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "fast",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 500 * time.Millisecond,
		BatchSize:    10000,
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(w *kafka.Writer) {
		<-c
		cleanup(w)
		os.Exit(1)
	}(w)

	timer := time.NewTicker(1 * time.Second)
	for {
		<-timer.C
		startTime := time.Now()
		messages := make([]kafka.Message, 0, 10000)
		for i := 1; i <= 10000; i++ {
			messages = append(messages,
				kafka.Message{
					Key:   []byte(fmt.Sprint(i)),
					Value: []byte("FastTest"),
				},
			)
		}
		log.Print(time.Since(startTime))
		log.Print(len(messages))
		err := w.WriteMessages(context.Background(), messages...)
		if err != nil {
			log.Print("failed to write messages:", err)
		}
		log.Print(time.Since(startTime))
	}

}

func cleanup(w *kafka.Writer) {

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
	log.Println("writer closed")
}
