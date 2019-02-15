package service

import (
	"github.com/rs/cors"
	"log"
	"net/http"
)

func StartWebServer(port string) {

	r := NewRouter()
	http.Handle("/", r)

	handler := cors.Default().Handler(r)

	log.Println("Starting Http service at " + port)
	err := http.ListenAndServe(":"+port, handler) // goroutine will block here

	if err != nil {
		log.Println("An error occured starting Http listener at port " + port)
		log.Println("Error: " + err.Error())
	}

}
