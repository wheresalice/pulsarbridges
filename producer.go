package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"io"
	"log"
	"os"
	"runtime"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
		OperationTimeoutSeconds: 5,
		MessageListenerThreads: runtime.NumCPU(),
	})

	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "bridges",
	})

	if err != nil {
		log.Printf("Could not instantiate Pulsar producer: %v", err)
	}

	defer producer.Close()

	ctx := context.Background()

	csvFile, _ := os.Open("2017HwyBridgesDelimitedAllStates.txt")
	reader := csv.NewReader(bufio.NewReader(csvFile))
	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Printf("Error parsing CSV: %s", error)
			continue
		}

		msg := pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("%s,%s", line[19], line[20])),
		}
		if err := producer.Send(ctx, msg); err != nil {
			log.Fatalf("Producer could not send message: %v", err)
		}
	}
}