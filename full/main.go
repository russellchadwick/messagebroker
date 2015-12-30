package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/russellchadwick/messagebroker"
)

type ExampleFullV1 struct {
	Test string `json:"test"`
}

var (
	dbHost        = flag.String("db-host", "localhost", "host for postgres database")
	dbUser        = flag.String("db-user", "messagebroker", "user for postgres database")
	dbPassword    = flag.String("db-password", "messagebroker", "password for postgres database")
	messageBroker messagebroker.MessageBroker
)

func main() {

	flag.Parse()

	var err error
	messageBroker, err = messagebroker.NewPostgresqlMessageBroker(*dbHost, *dbUser, *dbPassword)
	if err != nil {
		log.Fatalf("error creating message broker: %s \n", err)
	}

	go sendEvents()
	go messageBroker.Consume("Example.Full.1", onExampleMessage)

	for {

	}
}

func sendEvents() {
	for tick := range time.Tick(5 * time.Second) {

		exampleFullV1 := &ExampleFullV1{
			Test: "Test @ " + tick.String(),
		}

		exampleBytes, err := json.Marshal(exampleFullV1)
		if err != nil {
			log.Println("error marshaling event:", err)
		}

		err = messageBroker.Publish("Example.Full.1", exampleBytes)
		if err != nil {
			log.Println("error sending event:", err)
		}

	}
}

func onExampleMessage(body []byte) {
	var exampleFullV1 ExampleFullV1
	err := json.Unmarshal(body, &exampleFullV1)
	if err != nil {
		fmt.Println("Error unmarshal: ", err)
	}

	fmt.Println("Received example: ", exampleFullV1.Test)
}
