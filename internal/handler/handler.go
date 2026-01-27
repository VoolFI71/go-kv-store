package handler

import (
	"bufio"
	"net"
	"go-kv-store/internal/resp"
	"go-kv-store/internal/storage"
)

func HandleConn(conn net.Conn, st storage.Storage) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	buf := make([]byte, 64*1024)
	args := make([]string, 0, 16)

	for {
		err := resp.ParseArray(reader, buf, &args)
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			resp.WriteError(writer, "ERR invalid command format")
			writer.Flush()
			continue
		}

		if len(args) == 0 {
			continue
		}

		command := args[0]

		if command == "QUIT" || command == "quit" || command == "EXIT" || command == "exit" {
			resp.WriteString(writer, "OK")
			writer.Flush()
			return
		}

		if len(command) == 0 {
			resp.WriteError(writer, "ERR empty command")
			writer.Flush()
			continue
		}

		firstChar := command[0]
		if firstChar == 'S' || firstChar == 's' {
			if command == "SET" || command == "set" {
				if len(args) < 3 {
					resp.WriteError(writer, "ERR wrong number of arguments for 'SET' command")
				} else {
					key := args[1]
					value := args[2]
					st.Set(key, value)
					resp.WriteString(writer, "OK")
				}
				writer.Flush()
				continue
			}
		}

		if firstChar == 'G' || firstChar == 'g' {
			if command == "GET" || command == "get" {
				if len(args) < 2 {
					resp.WriteError(writer, "ERR wrong number of arguments for 'GET' command")
				} else {
					key := args[1]
					value, ok := st.Get(key)
					if ok {
						resp.WriteBulkString(writer, value)
					} else {
						resp.WriteNullBulkString(writer)
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
					resp.WriteError(writer, "ERR wrong number of arguments for 'CONFIG' command")
				}
				writer.Flush()
				continue
			}
		}

		if firstChar == 'P' || firstChar == 'p' {
			if command == "PING" || command == "ping" {
				resp.WriteString(writer, "PONG")
				writer.Flush()
				continue
			}
		}

		resp.WriteError(writer, "ERR unknown command '"+command+"'")
		writer.Flush()
	}
}
