package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {

	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Topic:        "pong",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 1000 * time.Millisecond,
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "ping",
		GroupID: "pong-group",
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func(w *kafka.Writer) {
		<-c
		cleanup(w, r)
		os.Exit(1)
	}(w)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			if string(m.Value) == "Ping" {
				log.Print("Ping received")
				time.Sleep(2 * time.Second)
				ping(w, "Pong")
			} else {
				log.Print("I win!")
			}
			//log.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		}
	}()

	// initial ping at the start
	//ping(w, "Pong")

	wg.Wait()

}

func cleanup(w *kafka.Writer, r *kafka.Reader) {

	ping(w, "You win")
	log.Println("I've lost =(")

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	log.Println("writer closed")

	if err := r.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	log.Println("writer closed")
}

func ping(w *kafka.Writer, message string) {
	err := w.WriteMessages(context.Background(),
		kafka.Message{
			//Key:       []byte(fmt.Sprint(i)),
			Value:     []byte(message),
			Partition: 1,
		},
	)
	if err != nil {
		log.Print("failed to write messages:", err)
	}
	log.Print("Pong")
}
