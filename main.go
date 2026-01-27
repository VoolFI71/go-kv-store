//go:build !benchmark
// +build !benchmark

package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func main() {
	storage := NewShardStorage()

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

		go handleConn(conn, storage)
	}
}


func handleConn(conn net.Conn, storage ShardStorage) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	buf := make([]byte, 64*1024)
	args := make([]string, 0, 16)

	for {
		err := parseRESPArray(reader, buf, &args)
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			writeRESPError(writer, "ERR invalid command format")
			writer.Flush()
			continue
		}

		if len(args) == 0 {
			continue
		}

		command := args[0]

		if command == "QUIT" || command == "quit" || command == "EXIT" || command == "exit" {
			writeRESPString(writer, "OK")
			writer.Flush()
			return
		}

		if len(command) == 0 {
			writeRESPError(writer, "ERR empty command")
			writer.Flush()
			continue
		}

		firstChar := command[0]
		if firstChar == 'S' || firstChar == 's' {
			if command == "SET" || command == "set" {
				if len(args) < 3 {
					writeRESPError(writer, "ERR wrong number of arguments for 'SET' command")
				} else {
					key := args[1]
					value := args[2]
					storage.Set(key, value)
					writeRESPString(writer, "OK")
				}
				writer.Flush()
				continue
			}
		}

		if firstChar == 'G' || firstChar == 'g' {
			if command == "GET" || command == "get" {
				if len(args) < 2 {
					writeRESPError(writer, "ERR wrong number of arguments for 'GET' command")
				} else {
					key := args[1]
					value, ok := storage.Get(key)
					if ok {
						writeRESPBulkString(writer, value)
					} else {
						writeRESPNullBulkString(writer)
					}
				}
				writer.Flush()
				continue
			}
		}

		if firstChar == 'C' || firstChar == 'c' {
			if command == "CONFIG" || command == "config" {
				if len(args) >= 2 && (args[1] == "GET" || args[1] == "get") {
					writer.WriteByte('*')
					writer.WriteString("0")
					writer.WriteString("\r\n")
				} else {
					writeRESPError(writer, "ERR wrong number of arguments for 'CONFIG' command")
				}
				writer.Flush()
				continue
			}
		}

		if firstChar == 'P' || firstChar == 'p' {
			if command == "PING" || command == "ping" {
				writeRESPString(writer, "PONG")
				writer.Flush()
				continue
			}
		}

		writeRESPError(writer, "ERR unknown command '"+command+"'")
		writer.Flush()
	}
}