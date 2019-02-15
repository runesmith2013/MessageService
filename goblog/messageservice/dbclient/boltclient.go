package dbclient

import (
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"log"
	"messageservice/goblog/messageservice/model"
	"strconv"
)

type IBoltClient interface {
	OpenBoltDb()
	QueryMessage(messageId string) (model.Message, error)
	Seed()
	InitializeBucket()
}

type BoltClient struct {
	boltDB *bolt.DB
}

func (bc *BoltClient) OpenBoltDb() {

	var err error
	bc.boltDB, err = bolt.Open("messages.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
}

// Start seeding messages
func (bc *BoltClient) Seed() {
	bc.initializeBucket()
	bc.seedMessages()
}

// Creates an "MessagesBucket" in our BoltDB. It will overwrite any existing bucket of the same name.
func (bc *BoltClient) initializeBucket() {
	bc.boltDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("MessagesBucket"))
		if err != nil {
			return fmt.Errorf("create bucket failed: %s", err)
		}
		return nil
	})
}

// seed N make believe messages into the MessagesBucket bucket
func (bc *BoltClient) seedMessages() {
	total := 10
	for i := 0; i < total; i++ {

		// Generate a key 10000 or larger
		key := strconv.Itoa(10000 + i)

		// Create an instance of our Message struct
		message := model.Message{
			Id:      key,
			Message: "Hello, " + strconv.Itoa(i),
		}

		// Serialize the struct to JSON
		jsonBytes, _ := json.Marshal(message)

		// Write the data to the MessageBucket
		bc.boltDB.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("MessagesBucket"))
			err := b.Put([]byte(key), jsonBytes)
			return err
		})
	}
	fmt.Printf("Seeded %v fake accounts...\n", total)
}

func (bc *BoltClient) QueryMessages() ([]model.Message, error) {
	// Allocate an empty Account instance we'll let json.Unmarhal populate for us in a bit.
	messages := make([]model.Message, 1)

	// Read an object from the bucket using boltDB.View
	err := bc.boltDB.View(func(tx *bolt.Tx) error {

		// Read the bucket from the DB
		b := tx.Bucket([]byte("MessagesBucket"))

		b.ForEach(func(k, v []byte) error {
			message := model.Message{}
			json.Unmarshal(v, &message)
			messages = append(messages, message)

			fmt.Println(message)
			return nil
		})

		// Return nil to indicate nothing went wrong, e.g no error
		return nil
	})
	// If there were an error, return the error
	if err != nil {
		return messages, err
	}
	// Return the Message struct and nil as error.
	return messages, nil
}
