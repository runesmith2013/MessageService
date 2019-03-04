package dbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"messageservice/goblog/messageservice/model"
	"testing"
	"time"
)

func TestReadFromRemote(t *testing.T) {

	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"3.8.198.171:9092"},
		Topic:     "test",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	r.SetOffset(0)
	messages := make([]model.Message, 0)

	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	for {

		m, err := r.ReadMessage(ctx)
		message := model.Message{}
		json.Unmarshal(m.Value, &message)
		messages = append(messages, message)

		if len(messages) > 10 {
			messages = messages[1:len(messages)]
		}

		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))

		if err != nil {

			break
		}
	}

	r.Close()

}

func TestReadFromTopic(t *testing.T) {

	kc := KafkaClient{Topic: "messages"}

	go kc.ConnectToTopic()

	time.Sleep(15 * time.Second)

	message := model.Message{

		Message: "This is a test",
		User:    "Test User",
		Topic:   "messages",
	}

	kc.AddMessage(message)

	messages, error := kc.GetMessages()

	if error != nil {
		t.Error("Unexpected Error reading from topic")
	}

	for message := range messages {
		fmt.Println(message, messages[message].Message)
	}

}
