package service

import (
	"github.com/gorilla/mux"
)

func NewRouter() *mux.Router {

	// create an instance of the gorilla router
	router := mux.NewRouter().StrictSlash(true)

	// iterate over the routes we declared in routes.go an attach them to the router interface

	for _, route := range routes {

		router.Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(route.HandlerFunc)
	}

	return router

}
