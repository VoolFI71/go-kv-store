//go:build !benchmark
// +build !benchmark

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/VoolFI71/go-kv-store/internal/handler"
	"github.com/VoolFI71/go-kv-store/internal/storage"
)

func main() {
	pprofAddr := flag.String("pprof", "localhost:9090", "pprof server address (empty to disable)")
	flag.Parse()

	st := storage.New()

	if *pprofAddr != "" {
		go func() {
			log.Printf("Starting pprof server on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("Warning: pprof server failed to start: %v", err)
			}
		}()
	}
	

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
