package main

import (
	"fmt"
	"messageservice/goblog/messageservice/dbclient"
	"messageservice/goblog/messageservice/service"
)

var appName = "Message Service"

func main() {
	intitializeMongoClient()

	fmt.Printf("Starting %v\n", appName)
	service.StartWebServer("6867")
}

func intitializeMongoClient() {
	service.DBClient = dbclient.KafkaClient{}
	//service.DBClient.OpenMongoDb()

}
