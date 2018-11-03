package main

import (
	"encoding/csv"
	"fmt"
	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"log"
	"runtime"
	"strconv"
	"strings"
	"image/color"

	"github.com/flopp/go-staticmaps"
	"github.com/fogleman/gg"
	"github.com/golang/geo/s2"
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

	msgChannel := make(chan pulsar.ConsumerMessage)
	consumerOpts := pulsar.ConsumerOptions{
		Topic:            "bridges",
		SubscriptionName: "my-subscription-1",
		Type:             pulsar.Exclusive,
		MessageChannel:   msgChannel,
	}
	consumer, err := client.Subscribe(consumerOpts)
	if err != nil {
		log.Fatalf("Could not establish subscription: %v", err)
	}

	defer consumer.Close()

	for cm := range msgChannel {
		msg := cm.Message

		r := csv.NewReader(strings.NewReader(string(msg.Payload())))
		record, err := r.Read()
		if err != nil {
			continue
		}
		//lat, _ := strconv.ParseFloat(record[0][:2] + "." + record[0][2:], 64)
		//lon, _ := strconv.ParseFloat("-" + record[1][:3] + "." + record[1][3:], 64)

		//39463090,105063467
		//(D)DDMMSS.ss
		latDegrees, _ := strconv.ParseFloat(record[0][:2], 64)
		latMinutes, _ := strconv.ParseFloat(record[0][2:4], 64)
		latSeconds, _ := strconv.ParseFloat(record[0][4:6], 64)
		latMilliseconds, _ := strconv.ParseFloat(record[0][6:], 64)
		lat := latDegrees + (latMinutes / 60) + (latSeconds / 3600) + (latMilliseconds / 3600 / 1000)

		lonDegrees, _ := strconv.ParseFloat(record[1][:3], 64)
		lonMinutes, _ := strconv.ParseFloat(record[1][3:5], 64)
		lonSeconds, _ := strconv.ParseFloat(record[1][5:7], 64)
		lonMilliseconds, _ := strconv.ParseFloat(record[1][7:], 64)
		lon := - (lonDegrees + (lonMinutes / 60) + (lonSeconds / 3600) + (lonMilliseconds / 3600 / 1000))

		log.Printf("https://www.openstreetmap.org/?mlat=%v&mlon=%v&zoom=11", lat, lon)


		ctx := sm.NewContext()
		ctx.SetSize(400, 300)
		ctx.AddMarker(sm.NewMarker(s2.LatLngFromDegrees(lat, lon), color.RGBA{0xff, 0, 0, 0xff}, 16.0))

		img, err := ctx.Render()
		if err != nil {
			panic(err)
		}

		if err := gg.SavePNG(fmt.Sprintf("images/%v.%v.png", lat, lon), img); err != nil {
			panic(err)
		}

		consumer.Ack(msg)
	}
}
