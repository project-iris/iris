// Iris - Distributed Messaging Framework
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
package session

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"github.com/karalabe/iris/config"
	"github.com/karalabe/iris/proto"
	"io"
	"net"
	"testing"
	"time"
)

func TestForward(t *testing.T) {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, _ := Listen(addr, serverKey, store)
	cliSes, _ := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	srvSes := <-sink

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 2)
	srvApp := make(chan *proto.Message, 2)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvNet := srvSes.Communicate(srvApp, quit) // Hack: reuse prev live quit channel

	// Generate the messages to transmit
	msgs := make([]proto.Message, 10)
	for i := 0; i < len(msgs); i++ {
		key := make([]byte, 20)
		iv := make([]byte, 20)
		data := make([]byte, 20)

		io.ReadFull(rand.Reader, key)
		io.ReadFull(rand.Reader, iv)
		io.ReadFull(rand.Reader, data)
		msgs[i] = proto.Message{proto.Header{[]byte("meta"), key, iv}, data}
	}
	// Send from client to server
	go func() {
		for i := 0; i < len(msgs); i++ {
			cliNet <- &msgs[i]
		}
	}()
	recvs := make([]proto.Message, 10)
	for i := 0; i < len(msgs); i++ {
		timeout := time.Tick(250 * time.Millisecond)
		select {
		case msg := <-srvApp:
			recvs[i] = *msg
		case <-timeout:
			t.Errorf("receive timed out")
			break
		}
	}
	for i := 0; i < 10; i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	// Send from server to client
	go func() {
		for i := 0; i < len(msgs); i++ {
			srvNet <- &msgs[i]
		}
	}()
	recvs = make([]proto.Message, 10)
	for i := 0; i < len(msgs); i++ {
		timeout := time.Tick(250 * time.Millisecond)
		select {
		case msg := <-cliApp:
			recvs[i] = *msg
		case <-timeout:
			t.Errorf("receive timed out")
			break
		}
	}
	for i := 0; i < 10; i++ {
		if bytes.Compare(msgs[i].Data, recvs[i].Data) != 0 || bytes.Compare(msgs[i].Head.Key, recvs[i].Head.Key) != 0 ||
			bytes.Compare(msgs[i].Head.Iv, recvs[i].Head.Iv) != 0 || bytes.Compare(msgs[i].Head.Meta.([]byte), []byte("meta")) != 0 {
			t.Errorf("send/receive mismatch: have %v, want %v.", recvs[i], msgs[i])
		}
	}
	close(quit)
}

func BenchmarkLatency1Byte(b *testing.B) {
	benchmarkLatency(b, 1)
}

func BenchmarkLatency4Byte(b *testing.B) {
	benchmarkLatency(b, 4)
}

func BenchmarkLatency16Byte(b *testing.B) {
	benchmarkLatency(b, 16)
}

func BenchmarkLatency64Byte(b *testing.B) {
	benchmarkLatency(b, 64)
}

func BenchmarkLatency256Byte(b *testing.B) {
	benchmarkLatency(b, 256)
}

func BenchmarkLatency1KByte(b *testing.B) {
	benchmarkLatency(b, 1024)
}

func BenchmarkLatency4KByte(b *testing.B) {
	benchmarkLatency(b, 4096)
}

func BenchmarkLatency16KByte(b *testing.B) {
	benchmarkLatency(b, 16384)
}

func BenchmarkLatency64KByte(b *testing.B) {
	benchmarkLatency(b, 65536)
}

func BenchmarkLatency256KByte(b *testing.B) {
	benchmarkLatency(b, 262144)
}

func BenchmarkLatency1MByte(b *testing.B) {
	benchmarkLatency(b, 1048576)
}

func benchmarkLatency(b *testing.B, block int) {
	// Setup the benchmark: public keys, stores and sessions
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, _ := Listen(addr, serverKey, store)
	cliSes, _ := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	srvSes := <-sink

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 64)
	srvApp := make(chan *proto.Message, 64)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvSes.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	// Create a header of the right size
	key := make([]byte, config.PacketCipherBits/8)
	io.ReadFull(rand.Reader, key)
	cipher, _ := config.PacketCipher(key)

	iv := make([]byte, cipher.BlockSize())
	io.ReadFull(rand.Reader, iv)

	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, key, iv}

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the client and server runner routines with a sync channel
	sync := make(chan struct{})
	done := make(chan struct{}, 2)

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			cliNet <- &msgs[i]
			<-sync
		}
		done <- struct{}{}
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-srvApp
			sync <- struct{}{}
		}
		done <- struct{}{}
	}
	// Send 1 message through to ensure internal caches are up
	cliNet <- &msgs[0]
	<-srvApp

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	go cliRun()
	go srvRun()

	<-done
	<-done

	b.StopTimer()
	close(quit)
}

func BenchmarkThroughput1Byte(b *testing.B) {
	benchmarkThroughput(b, 1)
}

func BenchmarkThroughput4Byte(b *testing.B) {
	benchmarkThroughput(b, 4)
}

func BenchmarkThroughput16Byte(b *testing.B) {
	benchmarkThroughput(b, 16)
}

func BenchmarkThroughput64Byte(b *testing.B) {
	benchmarkThroughput(b, 64)
}

func BenchmarkThroughput256Byte(b *testing.B) {
	benchmarkThroughput(b, 256)
}

func BenchmarkThroughput1KByte(b *testing.B) {
	benchmarkThroughput(b, 1024)
}

func BenchmarkThroughput4KByte(b *testing.B) {
	benchmarkThroughput(b, 4096)
}

func BenchmarkThroughput16KByte(b *testing.B) {
	benchmarkThroughput(b, 16384)
}

func BenchmarkThroughput64KByte(b *testing.B) {
	benchmarkThroughput(b, 65536)
}

func BenchmarkThroughput256KByte(b *testing.B) {
	benchmarkThroughput(b, 262144)
}

func BenchmarkThroughput1MByte(b *testing.B) {
	benchmarkThroughput(b, 1048576)
}

func benchmarkThroughput(b *testing.B, block int) {
	// Setup the benchmark: public keys, stores and sessions
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")

	serverKey, _ := rsa.GenerateKey(rand.Reader, 1024)
	clientKey, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["client"] = &clientKey.PublicKey

	sink, quit, _ := Listen(addr, serverKey, store)
	cliSes, _ := Dial("localhost", addr.Port, "client", clientKey, &serverKey.PublicKey)
	srvSes := <-sink

	// Create the sender and receiver channels for both session sides
	cliApp := make(chan *proto.Message, 64)
	srvApp := make(chan *proto.Message, 64)

	cliNet := cliSes.Communicate(cliApp, quit) // Hack: reuse prev live quit channel
	srvSes.Communicate(srvApp, quit)           // Hack: reuse prev live quit channel

	// Create a header of the right size
	key := make([]byte, config.PacketCipherBits/8)
	io.ReadFull(rand.Reader, key)
	cipher, _ := config.PacketCipher(key)

	iv := make([]byte, cipher.BlockSize())
	io.ReadFull(rand.Reader, iv)

	head := proto.Header{[]byte{0x99, 0x98, 0x97, 0x96}, key, iv}

	// Generate a large batch of random data to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head = head
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Create the client and server runner routines
	done := make(chan struct{}, 2)

	cliRun := func() {
		for i := 0; i < b.N; i++ {
			cliNet <- &msgs[i]
		}
		done <- struct{}{}
	}
	srvRun := func() {
		for i := 0; i < b.N; i++ {
			<-srvApp
		}
		done <- struct{}{}
	}
	// Send 1 message through to ensure internal caches are up
	cliNet <- &msgs[0]
	<-srvApp

	// Execute the client and server runners, wait till termination and exit
	b.ResetTimer()
	go cliRun()
	go srvRun()

	<-done
	<-done

	b.StopTimer()
	close(quit)
}

func BenchmarkOPPEThroughput1Byte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1, 1)
}

func BenchmarkOPPEThroughput4Byte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4, 1)
}

func BenchmarkOPPEThroughput16Byte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16, 1)
}

func BenchmarkOPPEThroughput64Byte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 64, 1)
}

func BenchmarkOPPEThroughput256Byte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 256, 1)
}

func BenchmarkOPPEThroughput1KByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1024, 1)
}

func BenchmarkOPPEThroughput4KByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4096, 1)
}

func BenchmarkOPPEThroughput16KByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16384, 1)
}

func BenchmarkOPPEThroughput64KByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 65536, 1)
}

func BenchmarkOPPEThroughput256KByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 262144, 1)
}

func BenchmarkOPPEThroughput1MByte1Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1048576, 1)
}

func BenchmarkOPPEThroughput1Byte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1, 2)
}

func BenchmarkOPPEThroughput4Byte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4, 2)
}

func BenchmarkOPPEThroughput16Byte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16, 2)
}

func BenchmarkOPPEThroughput64Byte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 64, 2)
}

func BenchmarkOPPEThroughput256Byte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 256, 2)
}

func BenchmarkOPPEThroughput1KByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1024, 2)
}

func BenchmarkOPPEThroughput4KByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4096, 2)
}

func BenchmarkOPPEThroughput16KByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16384, 2)
}

func BenchmarkOPPEThroughput64KByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 65536, 2)
}

func BenchmarkOPPEThroughput256KByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 262144, 2)
}

func BenchmarkOPPEThroughput1MByte2Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1048576, 2)
}

func BenchmarkOPPEThroughput1Byte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1, 4)
}

func BenchmarkOPPEThroughput4Byte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4, 4)
}

func BenchmarkOPPEThroughput16Byte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16, 4)
}

func BenchmarkOPPEThroughput64Byte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 64, 4)
}

func BenchmarkOPPEThroughput256Byte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 256, 4)
}

func BenchmarkOPPEThroughput1KByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1024, 4)
}

func BenchmarkOPPEThroughput4KByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4096, 4)
}

func BenchmarkOPPEThroughput16KByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16384, 4)
}

func BenchmarkOPPEThroughput64KByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 65536, 4)
}

func BenchmarkOPPEThroughput256KByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 262144, 4)
}

func BenchmarkOPPEThroughput1MByte4Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1048576, 4)
}

func BenchmarkOPPEThroughput1Byte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1, 8)
}

func BenchmarkOPPEThroughput4Byte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4, 8)
}

func BenchmarkOPPEThroughput16Byte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16, 8)
}

func BenchmarkOPPEThroughput64Byte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 64, 8)
}

func BenchmarkOPPEThroughput256Byte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 256, 8)
}

func BenchmarkOPPEThroughput1KByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1024, 8)
}

func BenchmarkOPPEThroughput4KByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 4096, 8)
}

func BenchmarkOPPEThroughput16KByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 16384, 8)
}

func BenchmarkOPPEThroughput64KByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 65536, 8)
}

func BenchmarkOPPEThroughput256KByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 262144, 8)
}

func BenchmarkOPPEThroughput1MByte8Mid(b *testing.B) {
	benchmarkOPPEThroughput(b, 1048576, 8)
}

// Benchmarks multi-hop message passing using OPPE mode
func benchmarkOPPEThroughput(b *testing.B, block int, midpoints int) {
	// Setup the RSA keys and key stores
	key, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["benchmark"] = &key.PublicKey

	// Set up the multi-hop chain
	addrs := make([]*net.TCPAddr, 0, midpoints+2)
	sinks := make([]chan *Session, 0, midpoints+2)
	quits := make([]chan struct{}, 0, midpoints+2)

	apps := make([]chan *proto.Message, 0, midpoints+2)
	nets := make([]chan *proto.Message, 0, midpoints+2)

	for i := 0; i < midpoints+2; i++ {
		addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
		sink, quit, _ := Listen(addr, key, store)

		addrs = append(addrs, addr)
		sinks = append(sinks, sink)
		quits = append(quits, quit)

		if i > 0 {
			// Dial each link
			cliSes, _ := Dial("localhost", addrs[i-1].Port, "benchmark", key, &key.PublicKey)
			srvSes := <-sinks[i-1]

			// Create the sender and receiver channels for both session sides
			cliApp := make(chan *proto.Message, 64)
			srvApp := make(chan *proto.Message, 64)

			cliNet := cliSes.Communicate(cliApp, quits[i]) // Hack: reuse prev live quit channel
			srvSes.Communicate(srvApp, quits[i-1])         // Hack: reuse prev live quit channel

			apps = append(apps, srvApp)
			nets = append(nets, cliNet)
		}
	}

	// Generate a large batch of random messages to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}

	sync := make(chan struct{})
	done := make(chan struct{})

	// Communication initiator
	go func() {
		<-sync
		for i := 0; i < b.N; i++ {
			msgs[i].Encrypt()
			nets[len(nets)-1] <- &msgs[i]
		}
		done <- struct{}{}
	}()

	// Midpoint forwarders
	for i := 1; i < midpoints+1; i++ {
		go func(idx int) {
			<-sync
			for i := 0; i < b.N; i++ {
				msg := <-apps[idx]
				nets[idx-1] <- msg
			}
			done <- struct{}{}
		}(i)
	}

	// Communication terminator
	go func() {
		<-sync
		for i := 0; i < b.N; i++ {
			msg := <-apps[0]
			msg.Decrypt()
		}
		done <- struct{}{}
	}()

	// Start the benchmark
	b.ResetTimer()
	for i := 0; i < midpoints+2; i++ {
		sync <- struct{}{}
	}
	for i := 0; i < midpoints+2; i++ {
		<-done
	}
	b.StopTimer()

	for _, quit := range quits {
		close(quit)
	}
	time.Sleep(100 * time.Millisecond)
}

func BenchmarkPPEThroughput1Byte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1, 1)
}

func BenchmarkPPEThroughput4Byte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4, 1)
}

func BenchmarkPPEThroughput16Byte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16, 1)
}

func BenchmarkPPEThroughput64Byte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 64, 1)
}

func BenchmarkPPEThroughput256Byte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 256, 1)
}

func BenchmarkPPEThroughput1KByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1024, 1)
}

func BenchmarkPPEThroughput4KByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4096, 1)
}

func BenchmarkPPEThroughput16KByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16384, 1)
}

func BenchmarkPPEThroughput64KByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 65536, 1)
}

func BenchmarkPPEThroughput256KByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 262144, 1)
}

func BenchmarkPPEThroughput1MByte1Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1048576, 1)
}

func BenchmarkPPEThroughput1Byte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1, 2)
}

func BenchmarkPPEThroughput4Byte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4, 2)
}

func BenchmarkPPEThroughput16Byte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16, 2)
}

func BenchmarkPPEThroughput64Byte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 64, 2)
}

func BenchmarkPPEThroughput256Byte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 256, 2)
}

func BenchmarkPPEThroughput1KByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1024, 2)
}

func BenchmarkPPEThroughput4KByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4096, 2)
}

func BenchmarkPPEThroughput16KByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16384, 2)
}

func BenchmarkPPEThroughput64KByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 65536, 2)
}

func BenchmarkPPEThroughput256KByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 262144, 2)
}

func BenchmarkPPEThroughput1MByte2Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1048576, 2)
}

func BenchmarkPPEThroughput1Byte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1, 4)
}

func BenchmarkPPEThroughput4Byte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4, 4)
}

func BenchmarkPPEThroughput16Byte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16, 4)
}

func BenchmarkPPEThroughput64Byte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 64, 4)
}

func BenchmarkPPEThroughput256Byte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 256, 4)
}

func BenchmarkPPEThroughput1KByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1024, 4)
}

func BenchmarkPPEThroughput4KByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4096, 4)
}

func BenchmarkPPEThroughput16KByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16384, 4)
}

func BenchmarkPPEThroughput64KByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 65536, 4)
}

func BenchmarkPPEThroughput256KByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 262144, 4)
}

func BenchmarkPPEThroughput1MByte4Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1048576, 4)
}

func BenchmarkPPEThroughput1Byte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1, 8)
}

func BenchmarkPPEThroughput4Byte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4, 8)
}

func BenchmarkPPEThroughput16Byte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16, 8)
}

func BenchmarkPPEThroughput64Byte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 64, 8)
}

func BenchmarkPPEThroughput256Byte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 256, 8)
}

func BenchmarkPPEThroughput1KByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1024, 8)
}

func BenchmarkPPEThroughput4KByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 4096, 8)
}

func BenchmarkPPEThroughput16KByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 16384, 8)
}

func BenchmarkPPEThroughput64KByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 65536, 8)
}

func BenchmarkPPEThroughput256KByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 262144, 8)
}

func BenchmarkPPEThroughput1MByte8Mid(b *testing.B) {
	benchmarkPPEThroughput(b, 1048576, 8)
}

// Benchmarks multi-hop message passing using OPPE mode
func benchmarkPPEThroughput(b *testing.B, block int, midpoints int) {
	// Setup the RSA keys and key stores
	key, _ := rsa.GenerateKey(rand.Reader, 1024)

	store := make(map[string]*rsa.PublicKey)
	store["benchmark"] = &key.PublicKey

	// Set up the multi-hop chain
	addrs := make([]*net.TCPAddr, 0, midpoints+2)
	sinks := make([]chan *Session, 0, midpoints+2)
	quits := make([]chan struct{}, 0, midpoints+2)

	apps := make([]chan *proto.Message, 0, midpoints+2)
	nets := make([]chan *proto.Message, 0, midpoints+2)

	for i := 0; i < midpoints+2; i++ {
		addr, _ := net.ResolveTCPAddr("tcp", "localhost:0")
		sink, quit, _ := Listen(addr, key, store)

		addrs = append(addrs, addr)
		sinks = append(sinks, sink)
		quits = append(quits, quit)

		if i > 0 {
			// Dial each link
			cliSes, _ := Dial("localhost", addrs[i-1].Port, "benchmark", key, &key.PublicKey)
			srvSes := <-sinks[i-1]

			// Create the sender and receiver channels for both session sides
			cliApp := make(chan *proto.Message, 64)
			srvApp := make(chan *proto.Message, 64)

			cliNet := cliSes.Communicate(cliApp, quits[i]) // Hack: reuse prev live quit channel
			srvSes.Communicate(srvApp, quits[i-1])         // Hack: reuse prev live quit channel

			apps = append(apps, srvApp)
			nets = append(nets, cliNet)
		}
	}

	// Generate a large batch of random messages to forward
	b.SetBytes(int64(block))
	msgs := make([]proto.Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Head.Meta = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Head.Meta.([]byte))
	}

	sync := make(chan struct{})
	done := make(chan struct{})

	// Communication initiator
	go func() {
		<-sync
		for i := 0; i < b.N; i++ {
			nets[len(nets)-1] <- &msgs[i]
		}
		done <- struct{}{}
	}()

	// Midpoint forwarders
	for i := 1; i < midpoints+1; i++ {
		go func(idx int) {
			<-sync
			for i := 0; i < b.N; i++ {
				msg := <-apps[idx]
				nets[idx-1] <- msg
			}
			done <- struct{}{}
		}(i)
	}

	// Communication terminator
	go func() {
		<-sync
		for i := 0; i < b.N; i++ {
			<-apps[0]
		}
		done <- struct{}{}
	}()

	// Start the benchmark
	b.ResetTimer()
	for i := 0; i < midpoints+2; i++ {
		sync <- struct{}{}
	}
	for i := 0; i < midpoints+2; i++ {
		<-done
	}
	b.StopTimer()

	for _, quit := range quits {
		close(quit)
	}
	time.Sleep(100 * time.Millisecond)
}
