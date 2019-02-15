package model

type Message struct {
	Id      string `json:"id"`
	Message string `json:"message"`
	User    string `json:"user"`
	Topic   string `json:"topic"`
}
