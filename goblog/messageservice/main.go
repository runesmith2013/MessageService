package main

import (
	"fmt"
	"messageservice/goblog/messageservice/dbclient"
	"messageservice/goblog/messageservice/service"
)

var appName = "Message Service"

func main() {
	initialiseKafkaClient()

	fmt.Printf("Starting %v\n", appName)
	service.StartWebServer("6867")
}

func initialiseKafkaClient() {

	topics := [5]string{"test0", "test1", "test2", "test3", "test4"}

	for i := 0; i < len(topics); i++ {

		topic := topics[i]
		client := dbclient.KafkaClient{Topic: topic}
		service.KafkaClients[topic] = &client

		go client.ConnectToTopic()
		fmt.Println("Connecting to ", topics[i])
	}

}
