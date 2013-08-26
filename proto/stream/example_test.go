// Iris - Decentralized Messaging Framework
// Copyright 2013 Peter Szilagyi. All rights reserved.
//
// Iris is dual licensed: you can redistribute it and/or modify it under the
// terms of the GNU General Public License as published by the Free Software
// Foundation, either version 3 of the License, or (at your option) any later
// version.
//
// The framework is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
// more details.
//
// Alternatively, the Iris framework may be used in accordance with the terms
// and conditions contained in a signed written agreement between you and the
// author(s).
//
// Author: peterke@gmail.com (Peter Szilagyi)
package stream_test

import (
	"fmt"
	"github.com/karalabe/iris/proto/stream"
	"net"
)

var host = "localhost"
var port = 55555

// Stream example of an echo server and client using streams.
func Example_usage() {
	quit := make(chan struct{})
	data := make(chan string)
	msg := "Hello Stream!"

	go server(quit)
	go client(msg, data)

	fmt.Println("Input message:", msg)
	fmt.Println("Output message:", <-data)

	close(quit)
	// Output:
	// Input message: Hello Stream!
	// Output message: Hello Stream!
}

func server(quit chan struct{}) {
	// Open a TCP port to accept incoming stream connections
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Println("Failed to resolve local address:", err)
		return
	}
	sink, strmQuit, err := stream.Listen(addr)
	if err != nil {
		fmt.Println("Failed to listen for incoming streams:", err)
		return
	}
	// While not exiting, process stream connections
	select {
	case <-quit:
		close(strmQuit)
		return
	case strm := <-sink:
		// Receive and echo back a string
		data := new(string)
		err = strm.Recv(data)
		if err != nil {
			fmt.Println("failed to receive a string object:", err)
		} else {
			err = strm.Send(data)
			if err != nil {
				fmt.Println("failed to send back a string object:", err)
			}
			strm.Flush()
			if err != nil {
				fmt.Println("Failed to flush the response:", err)
				return
			}
		}
		// Close the stream
		strm.Close()
	}
}

func client(msg string, ch chan string) {
	// Open a TCP connection to the stream server
	strm, err := stream.Dial(host, port)
	if err != nil {
		fmt.Println("Failed to connect to stream server:", err)
		return
	}
	// Send the message and receive a reply
	err = strm.Send(msg)
	if err != nil {
		fmt.Println("Failed to send the message:", err)
		return
	}
	err = strm.Flush()
	if err != nil {
		fmt.Println("Failed to flush the message:", err)
		return
	}
	err = strm.Recv(&msg)
	if err != nil {
		fmt.Println("Failed to receive the reply:", err)
		return
	}
	// Return the reply to the caller and terminate
	ch <- msg
	strm.Close()
}
