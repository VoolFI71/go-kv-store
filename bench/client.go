package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
)

type BenchmarkClient struct {
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	scratch []byte
}

func NewBenchmarkClient(addr string) (*BenchmarkClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &BenchmarkClient{
		conn:    conn,
		reader:  bufio.NewReaderSize(conn, 64*1024),
		writer:  bufio.NewWriterSize(conn, 64*1024),
		scratch: make([]byte, 64*1024),
	}, nil
}

func (c *BenchmarkClient) writeRESPCommand(args ...string) error {
	c.writeRESPCommandNoFlush(args...)
	return c.writer.Flush()
}

func (c *BenchmarkClient) writeRESPCommandNoFlush(args ...string) {
	c.writer.WriteByte('*')
	c.writer.WriteString(strconv.Itoa(len(args)))
	c.writer.WriteString("\r\n")

	for _, arg := range args {
		c.writer.WriteByte('$')
		c.writer.WriteString(strconv.Itoa(len(arg)))
		c.writer.WriteString("\r\n")
		c.writer.WriteString(arg)
		c.writer.WriteString("\r\n")
	}
}

func (c *BenchmarkClient) Flush() error {
	return c.writer.Flush()
}

func parseIntLine(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty number")
	}
	i := 0
	neg := false
	if data[i] == '-' {
		neg = true
		i++
		if i >= len(data) {
			return 0, fmt.Errorf("no digits after minus")
		}
	}
	n := 0
	found := false
	for ; i < len(data); i++ {
		b := data[i]
		if b == '\r' || b == '\n' {
			break
		}
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid digit: %c", b)
		}
		found = true
		n = n*10 + int(b-'0')
	}
	if !found {
		return 0, fmt.Errorf("no digits found")
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

func (c *BenchmarkClient) readRESPResponse() (string, error) {
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			line, err = c.reader.ReadBytes('\n')
		}
		return "", err
	}

	if len(line) < 3 {
		return "", fmt.Errorf("invalid RESP response")
	}

	switch line[0] {
	case '+':
		return string(bytes.TrimSpace(line[1:])), nil
	case '-':
		return "", fmt.Errorf("server error: %s", string(bytes.TrimSpace(line[1:])))
	case '$':
		length, err := parseIntLine(line[1:])
		if err != nil {
			return "", err
		}
		if length == -1 {
			return "", nil
		}
		need := length + 2
		var data []byte
		if need <= len(c.scratch) {
			data = c.scratch[:need]
		} else {
			data = make([]byte, need)
		}
		_, err = io.ReadFull(c.reader, data)
		if err != nil {
			return "", err
		}
		return string(data[:length]), nil
	default:
		return "", fmt.Errorf("unexpected RESP type: %c", line[0])
	}
}

func (c *BenchmarkClient) skipRESPResponse() error {
	line, err := c.reader.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			line, err = c.reader.ReadBytes('\n')
		}
		if err != nil {
			return err
		}
	}

	if len(line) < 3 {
		return fmt.Errorf("invalid RESP response")
	}

	switch line[0] {
	case '+', '-':
		return nil
	case '$':
		length, err := parseIntLine(line[1:])
		if err != nil {
			return err
		}
		if length == -1 {
			return nil
		}
		need := length + 2
		if need <= len(c.scratch) {
			_, err = io.ReadFull(c.reader, c.scratch[:need])
			return err
		}
		_, err = io.CopyN(io.Discard, c.reader, int64(need))
		return err
	default:
		return fmt.Errorf("unexpected RESP type: %c", line[0])
	}
}

func (c *BenchmarkClient) Set(key, value string) error {
	err := c.writeRESPCommand("SET", key, value)
	if err != nil {
		return err
	}
	_, err = c.readRESPResponse()
	return err
}

func (c *BenchmarkClient) Get(key string) (string, error) {
	err := c.writeRESPCommand("GET", key)
	if err != nil {
		return "", err
	}
	return c.readRESPResponse()
}

func (c *BenchmarkClient) SetPipeline(key, value string) {
	c.writeRESPCommandNoFlush("SET", key, value)
}

func (c *BenchmarkClient) GetPipeline(key string) {
	c.writeRESPCommandNoFlush("GET", key)
}

func (c *BenchmarkClient) ReadPipelineResponses(count int) error {
	for i := 0; i < count; i++ {
		if err := c.skipRESPResponse(); err != nil {
			return err
		}
	}
	return nil
}

func (c *BenchmarkClient) Close() error {
	c.writeRESPCommand("QUIT")
	c.readRESPResponse()
	return c.conn.Close()
}
