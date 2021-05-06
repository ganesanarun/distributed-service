package main

import (
	"log"

	"github.com/ganesanarun/proglog/internal/server"
)

func main() {
	serve := server.NewHttpServer(":8080")
	log.Fatal(serve.ListenAndServe())
}