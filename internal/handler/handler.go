package handler

import (
	"bufio"
	"github.com/VoolFI71/go-kv-store/internal/resp"
	"github.com/VoolFI71/go-kv-store/internal/storage"
	"net"
	"strconv"
)

func HandleConn(conn net.Conn, st storage.Storage) {
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 1024*1024)

	args := make([]string, 0, 32)
	cmdBuf := make([]byte, 0, 64*1024)

	const maxResponsesBeforeFlush = 128
	responsesSinceFlush := 0

	flushIfBatchDone := func() {
		// Если во входящем буфере нет данных, значит текущая пачка команд обработана
		// и можно отправить накопленные ответы.
		if reader.Buffered() == 0 || responsesSinceFlush >= maxResponsesBeforeFlush {
			_ = writer.Flush()
			responsesSinceFlush = 0
		}
	}

	for {
		err := resp.ParseArray(reader, &args, &cmdBuf)
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			resp.WriteError(writer, "ERR invalid command format")
			_ = writer.Flush()
			responsesSinceFlush = 0
			continue
		}

		if len(args) == 0 {
			continue
		}

		command := args[0]

		if command == "QUIT" || command == "quit" || command == "EXIT" || command == "exit" {
			resp.WriteString(writer, "OK")
			_ = writer.Flush()
			responsesSinceFlush = 0
			return
		}

		if len(command) == 0 {
			resp.WriteError(writer, "ERR empty command")
			responsesSinceFlush++
			flushIfBatchDone()
			continue
		}

		firstChar := command[0]
		if firstChar == 'S' || firstChar == 's' {
			if command == "SET" || command == "set" {
				if len(args) < 3 {
					resp.WriteError(writer, "ERR wrong number of arguments for 'SET' command")
				} else {
					key := string([]byte(args[1]))
					value := string([]byte(args[2]))
					st.Set(key, value)
					resp.WriteString(writer, "OK")
				}
				responsesSinceFlush++
				flushIfBatchDone()
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
				responsesSinceFlush++
				flushIfBatchDone()
				continue
			}
		}

		if firstChar == 'I' || firstChar == 'i' {
			if command == "INCR" || command == "incr" {
				if len(args) < 2 {
					resp.WriteError(writer, "ERR wrong number of arguments for 'INCR' command")
				} else {
					key := string([]byte(args[1]))
					value, err := st.Incr(key)
					if err != nil {
						resp.WriteError(writer, err.Error())
					} else {
						resp.WriteInt(writer, value)
					}
				}
				responsesSinceFlush++
				flushIfBatchDone()
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
				responsesSinceFlush++
				flushIfBatchDone()
				continue
			}
		}

		if firstChar == 'E' || firstChar == 'e' {
			if command == "EXPIRE" || command == "expire" {
				if len(args) < 3 {
					resp.WriteError(writer, "ERR wrong number of arguments for 'EXPIRE' command")
				} else {
					seconds, err := strconv.ParseInt(args[2], 10, 64)
					if err != nil {
						resp.WriteError(writer, "ERR value is not an integer or out of range")
					} else {
						ok := st.SetExpire(string([]byte(args[1])), seconds)
						if ok {
							resp.WriteInt(writer, 1)
						} else {
							resp.WriteInt(writer, 0)
						}
					}
				}
				responsesSinceFlush++
				flushIfBatchDone()
				continue
			}
		}

		if firstChar == 'P' || firstChar == 'p' {
			if command == "PING" || command == "ping" {
				resp.WriteString(writer, "PONG")
				responsesSinceFlush++
				flushIfBatchDone()
				continue
			}
		}

		resp.WriteError(writer, "ERR unknown command '"+command+"'")
		responsesSinceFlush++
		flushIfBatchDone()
	}
}
