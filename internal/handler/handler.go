package handler

import (
	"bufio"
	"go-kv-store/internal/resp"
	"go-kv-store/internal/storage"
	"net"
)

func HandleConn(conn net.Conn, st storage.Storage) {
	defer conn.Close()
	// Larger buffers matter a lot for pipelining (batch responses can exceed default 4KB).
	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 1024*1024)

	buf := make([]byte, 64*1024)
	args := make([]string, 0, 32)

	// Max responses to buffer before forcing a flush.
	// Keep it >= typical pipeline batch size to avoid splitting a batch into multiple flushes.
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
		err := resp.ParseArray(reader, buf, &args)
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
					key := args[1]
					value := args[2]
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
