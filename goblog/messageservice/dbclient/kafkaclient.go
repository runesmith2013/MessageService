package dbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"messageservice/goblog/messageservice/model"
	"strconv"
)

type KafkaClient struct {
	messages []model.Message
}

func (kc *KafkaClient) Connect() {
	kc.messages = make([]model.Message, 0)

	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "messages",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	r.SetOffset(1)
	fmt.Println("Length of queue", r.Stats().QueueLength)

	for {

		m, err := r.ReadMessage(context.Background())
		message := model.Message{}
		json.Unmarshal(m.Value, &message)
		kc.messages = append(kc.messages, message)

		fmt.Println("Length of messages: ", len(kc.messages))

		if len(kc.messages) > 10 {
			kc.messages = kc.messages[1:len(kc.messages)]

		}

		if err != nil {
			//error = err
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()

}

func (kc *KafkaClient) GetMessages() ([]model.Message, error) {

	return kc.messages, nil

}

func (kc *KafkaClient) AddMessage(message model.Message) error {

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "messages",
	})

	// Serialize the struct to JSON
	jsonBytes, _ := json.Marshal(message)

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(message.Id),
			Value: []byte(jsonBytes),
		},
	)

	return err

}

func ExampleWriter() {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	})

	for i := 1; i <= 5000; i++ {

		key := strconv.Itoa(i)
		message := "Hello World " + key
		w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(key),
				Value: []byte(message),
			},
		)
	}

	w.Close()
}

func ExampleReader() {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "test",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(42)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
