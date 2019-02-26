package service

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"messageservice/goblog/messageservice/dbclient"

	"log"

	"messageservice/goblog/messageservice/model"
	"net/http"
)

var KafkaClients = make(map[string]*dbclient.KafkaClient)

func responseWithJSON(w http.ResponseWriter, json []byte, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	w.Write(json)
}

func errorWithJSON(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprintf(w, "{message: %q}", message)
}

func GetMessagesByTopic(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	topic := vars["topic"]

	fmt.Println()

	client := KafkaClients[topic]
	messages, err := client.GetMessages()

	if err != nil {
		errorWithJSON(w, "Database error", http.StatusInternalServerError)
		log.Println("Failed get all messages: ", err)
		return
	}

	// If found, marshal into JSON, write headers and content
	respBody, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	responseWithJSON(w, respBody, http.StatusOK)

}

func AddMessageToTopic(w http.ResponseWriter, r *http.Request) {

	var message model.Message
	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&message)
	if err != nil {
		errorWithJSON(w, "Incorrect body", http.StatusBadRequest)
		return
	}

	topic := message.Topic
	client := KafkaClients[topic]
	err = client.AddMessage(message)
	if err != nil {
		errorWithJSON(w, "Database error", http.StatusInternalServerError)
		log.Println("Failed to write messages: ", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", r.URL.Path+"/"+message.Id)
	w.WriteHeader(http.StatusCreated)
}
