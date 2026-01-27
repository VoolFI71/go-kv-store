//go:build !benchmark
// +build !benchmark

package main

import (
	"fmt"
	"log"
	"net"

	"go-kv-store/internal/handler"
	"go-kv-store/internal/storage"
)

func main() {
	st := storage.New()

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	fmt.Println("KV Store server started on 0.0.0.0:6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go handler.HandleConn(conn, st)
	}
}
