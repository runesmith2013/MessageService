package service

import (
	"encoding/json"
	"fmt"
	"messageservice/goblog/messageservice/dbclient"

	"log"

	"messageservice/goblog/messageservice/model"
	"net/http"
)

var DBClient dbclient.KafkaClient

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

func GetAllMessages(w http.ResponseWriter, r *http.Request) {

	// Read the message struct
	messages, err := DBClient.GetMessages()

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

func AddMessage(w http.ResponseWriter, r *http.Request) {

	var message model.Message
	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&message)
	if err != nil {
		errorWithJSON(w, "Incorrect body", http.StatusBadRequest)
		return
	}

	err = DBClient.AddMessage(message)
	if err != nil {
		errorWithJSON(w, "Database error", http.StatusInternalServerError)
		log.Println("Failed get all messages: ", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Location", r.URL.Path+"/"+message.Id)
	w.WriteHeader(http.StatusCreated)
}
