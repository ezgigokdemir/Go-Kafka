package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main() {

	var message string
	var count int
	flag.StringVar(&message, "message", "", "write your message")
	flag.IntVar(&count, "count", 0, "write your count")
	flag.Parse()

	ctx := context.Background()

	go producer(ctx, message, count)
	consumer(ctx)
}

func producer(ctx context.Context, message string, count int) {

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "test-topic",
		Balancer: &kafka.LeastBytes{},
	}

	for i := 1; i <= count; i++ {
		err := writer.WriteMessages(ctx,
			kafka.Message{
				Value: []byte(message + " " + strconv.Itoa(i)),
			},
		)

		if err != nil {
			fmt.Println("An error is occured: ", err)
			continue
		}

		fmt.Println("Message sent.")
	}
}

func consumer(ctx context.Context) {
	conf := kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "test-topic",
		GroupID:  "G1",
		MaxBytes: 100,
	}

	reader := kafka.NewReader(conf)

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println("An error is occured: ", err)
		} else {
			fmt.Println("Message: ", string(m.Value))
		}
	}
}
