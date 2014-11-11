// Iris - Decentralized cloud messaging
// Copyright (c) 2013 Project Iris. All rights reserved.
//
// Community license: for open source projects and services, Iris is free to use,
// redistribute and/or modify under the terms of the GNU Affero General Public
// License as published by the Free Software Foundation, either version 3, or (at
// your option) any later version.
//
// Evaluation license: you are free to privately evaluate Iris without adhering
// to either of the community or commercial licenses for as long as you like,
// however you are not permitted to publicly release any software or service
// built on top of it without a valid license.
//
// Commercial license: for commercial and/or closed source projects and services,
// the Iris cloud messaging system may be used in accordance with the terms and
// conditions contained in an individually negotiated signed written agreement
// between you and the author(s).

package proto

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func TestCrypto(t *testing.T) {
	for length := 1; length <= 1024*1024; length *= 4 {
		// Generate a message of the given length
		data := make([]byte, length)
		if n, err := io.ReadFull(rand.Reader, data); n != len(data) || err != nil {
			t.Fatalf("failed to generate random message: %v.", err)
		}
		msg := Message{
			Data: data,
		}
		// Copy and encrypt the original message
		cpy := Message{
			Data: make([]byte, length),
		}
		copy(cpy.Data, msg.Data)

		if err := cpy.Encrypt(); err != nil {
			t.Fatalf("failed to encrypt message %v: %v.", cpy.Data, err)
		}
		// Make sure the encrypted message differs from the original and that the key and IV is present
		if bytes.Compare(msg.Data, cpy.Data) == 0 {
			t.Fatalf("data not encrypted.")
		}
		if cpy.Head.Key == nil || cpy.Head.Iv == nil {
			t.Fatalf("encryption fields missing: key = %v, iv = %v.", cpy.Head.Key, cpy.Head.Iv)
		}
		// Decrypt the encrypted message and check match with original
		if err := cpy.Decrypt(); err != nil {
			t.Fatalf("failed to dencrypt message %v: %v.", cpy, err)
		}
		if bytes.Compare(msg.Data, cpy.Data) != 0 {
			t.Fatalf("message data mismatch: have %v, want %v.", cpy.Data, msg.Data)
		}
		if cpy.Head.Key != nil || cpy.Head.Iv != nil {
			t.Fatalf("encryption leftover fields: key = %v, iv = %v.", cpy.Head.Key, cpy.Head.Iv)
		}
	}
}

func BenchmarkEncrypt1Byte(b *testing.B) {
	benchmarkEncrypt(b, 1)
}

func BenchmarkEncrypt4Byte(b *testing.B) {
	benchmarkEncrypt(b, 4)
}

func BenchmarkEncrypt16Byte(b *testing.B) {
	benchmarkEncrypt(b, 16)
}

func BenchmarkEncrypt64Byte(b *testing.B) {
	benchmarkEncrypt(b, 64)
}

func BenchmarkEncrypt256Byte(b *testing.B) {
	benchmarkEncrypt(b, 256)
}

func BenchmarkEncrypt1KByte(b *testing.B) {
	benchmarkEncrypt(b, 1024)
}

func BenchmarkEncrypt4KByte(b *testing.B) {
	benchmarkEncrypt(b, 4096)
}

func BenchmarkEncrypt16KByte(b *testing.B) {
	benchmarkEncrypt(b, 16384)
}

func BenchmarkEncrypt64KByte(b *testing.B) {
	benchmarkEncrypt(b, 65536)
}

func BenchmarkEncrypt256KByte(b *testing.B) {
	benchmarkEncrypt(b, 262144)
}

func BenchmarkEncrypt1MByte(b *testing.B) {
	benchmarkEncrypt(b, 1048576)
}

func benchmarkEncrypt(b *testing.B, block int) {
	// Generate a large batch of random data to encrypt
	b.SetBytes(int64(block))
	msgs := make([]Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Reset the timer and encrypt the messages
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs[i].Encrypt()
	}
}

func BenchmarkDecrypt1Byte(b *testing.B) {
	benchmarkDecrypt(b, 1)
}

func BenchmarkDecrypt4Byte(b *testing.B) {
	benchmarkDecrypt(b, 4)
}

func BenchmarkDecrypt16Byte(b *testing.B) {
	benchmarkDecrypt(b, 16)
}

func BenchmarkDecrypt64Byte(b *testing.B) {
	benchmarkDecrypt(b, 64)
}

func BenchmarkDecrypt256Byte(b *testing.B) {
	benchmarkDecrypt(b, 256)
}

func BenchmarkDecrypt1KByte(b *testing.B) {
	benchmarkDecrypt(b, 1024)
}

func BenchmarkDecrypt4KByte(b *testing.B) {
	benchmarkDecrypt(b, 4096)
}

func BenchmarkDecrypt16KByte(b *testing.B) {
	benchmarkDecrypt(b, 16384)
}

func BenchmarkDecrypt64KByte(b *testing.B) {
	benchmarkDecrypt(b, 65536)
}

func BenchmarkDecrypt256KByte(b *testing.B) {
	benchmarkDecrypt(b, 262144)
}

func BenchmarkDecrypt1MByte(b *testing.B) {
	benchmarkDecrypt(b, 1048576)
}

func benchmarkDecrypt(b *testing.B, block int) {
	// Generate a large batch of random data to encrypt
	b.SetBytes(int64(block))
	msgs := make([]Message, b.N)
	for i := 0; i < b.N; i++ {
		msgs[i].Data = make([]byte, block)
		io.ReadFull(rand.Reader, msgs[i].Data)
	}
	// Encrypt the messages
	for i := 0; i < b.N; i++ {
		msgs[i].Encrypt()
	}
	// Reset the timer and time the decryption
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgs[i].Decrypt()
	}
}
