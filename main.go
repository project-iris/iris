package main

import (
	"crypto/rand"
	"crypto/rsa"
	"github.com/karalabe/iris/proto/carrier"
	"github.com/karalabe/iris/service/relay"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	relayPort := 55555

	// Generate a new temporary key for the carrier
	key, _ := rsa.GenerateKey(rand.Reader, 512)

	// Create and boot a new carrier
	log.Printf("main: booting carrier...")
	car := carrier.New("iris", key)
	if err := car.Boot(); err != nil {
		panic(err)
	}
	// Wait for boot to complete
	time.Sleep(time.Second)

	// Create and boot a new relay
	log.Printf("main: booting relay service...")
	rel, err := relay.New(relayPort, car)
	if err != nil {
		panic(err)
	}
	rel.Boot()

	// Capture termination signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Report success
	log.Printf("main: iris successfully booted, listening on port %d.", relayPort)

	// Wait for termination request, clean up and exit
	<-quit
	log.Printf("main: terminating relay service...")
	if err := rel.Terminate(); err != nil {
		log.Printf("main: relay service termination failure: %v.", err)
	}
	log.Printf("main: terminating carrier...")
	car.Shutdown()
	log.Printf("main: iris terminated.")
}
