package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"github.com/karalabe/iris/proto/carrier"
	"github.com/karalabe/iris/service/relay"
	"io/ioutil"
	"log"
	rng "math/rand"
	"os"
	"os/signal"
	"time"
)

// Command line flags
var devMode = flag.Bool("dev", false, "start in local developer mode (random cluster and key)")
var relayPort = flag.Int("port", 55555, "relay endpoint for locally connecting clients")
var clusterName = flag.String("net", "", "name of the cluster to join or create")
var rsaKeyPath = flag.String("rsa", "", "path to the RSA private key to use for data security")

// Parses the command line flags and checks their validity
func parseFlags() (int, string, *rsa.PrivateKey) {
	var rsaKey *rsa.PrivateKey

	// Read the command line arguments
	flag.Parse()

	// Check the relay port range
	if *relayPort <= 0 || *relayPort >= 65536 {
		fmt.Fprintf(os.Stderr, "Invalid relay port: have %v, want [1-65535]\n.", *relayPort)
		os.Exit(-1)
	}
	// User random cluster id and RSA key in developer mode
	if *devMode {
		// Generate a secure RSA key
		fmt.Printf("Entering developer mode\n")
		fmt.Printf("Generating random RSA key... ")
		if key, err := rsa.GenerateKey(rand.Reader, 2048); err != nil {
			fmt.Printf("failed: %v\n", err)
			os.Exit(-2)
		} else {
			fmt.Printf("done.\n")
			rsaKey = key
		}
		// Generate a probably unique cluster name
		fmt.Printf("Generating random cluster name... ")
		*clusterName = fmt.Sprintf("dev-cluster-%v", rng.Int63())
		fmt.Printf("done.\n")
		fmt.Println()
	} else {
		// Production mode, read the cluster id and RSA key from teh arguments
		if *clusterName == "" {
			fmt.Fprintf(os.Stderr, "No cluster specified (-net), did you intend developer mode (-dev)?\n")
			os.Exit(-1)
		}
		if *rsaKeyPath == "" {
			fmt.Fprintf(os.Stderr, "No RSA key specified (-rsa), did you intend developer mode (-dev)?\n")
			os.Exit(-1)
		}
		if rsaData, err := ioutil.ReadFile(*rsaKeyPath); err != nil {
			fmt.Fprintf(os.Stderr, "Reading RSA key failed: %v.\n", err)
			os.Exit(-1)
		} else {
			// Try processing as PEM format
			if block, _ := pem.Decode(rsaData); block != nil {
				if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err != nil {
					fmt.Fprintf(os.Stderr, "Parsing RSA key from PEM format failed: %v.\n", err)
					os.Exit(-1)
				} else {
					rsaKey = key
				}
			} else {
				// Give it a shot as simple binary DER
				if key, err := x509.ParsePKCS1PrivateKey(rsaData); err != nil {
					fmt.Fprintf(os.Stderr, "Failed to parse RSA key from both PEM and DER format.\n")
					os.Exit(-1)
				} else {
					rsaKey = key
				}
			}
		}
	}
	return *relayPort, *clusterName, rsaKey
}

func main() {
	// Extract the command line arguments
	relayPort, clusterId, rsaKey := parseFlags()

	// Create and boot a new carrier
	log.Printf("main: booting carrier...")
	car := carrier.New(clusterId, rsaKey)
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
