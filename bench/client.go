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
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewBenchmarkClient(addr string) (*BenchmarkClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &BenchmarkClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
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

func (c *BenchmarkClient) readRESPResponse() (string, error) {
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
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
		length, err := strconv.Atoi(string(bytes.TrimSpace(line[1:])))
		if err != nil {
			return "", err
		}
		if length == -1 {
			return "", nil
		}
		data := make([]byte, length+2)
		_, err = io.ReadFull(c.reader, data)
		if err != nil {
			return "", err
		}
		return string(data[:length]), nil
	default:
		return "", fmt.Errorf("unexpected RESP type: %c", line[0])
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
		_, err := c.readRESPResponse()
		if err != nil {
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
