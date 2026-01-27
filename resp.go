package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP типы
const (
	RESPString     = '+' // Простая строка
	RESPError      = '-' // Ошибка
	RESPBulkString = '$' // Bulk string
	RESPArray      = '*' // Массив
)

// parseInt парсит число из байтового среза
func parseInt(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty number")
	}

	start := 0
	for start < len(data) && (data[start] == ' ' || data[start] == '\t') {
		start++
	}

	if start >= len(data) {
		return 0, fmt.Errorf("no digits found")
	}

	negative := false
	if data[start] == '-' {
		negative = true
		start++
		if start >= len(data) {
			return 0, fmt.Errorf("no digits after minus")
		}
	}

	result := 0
	foundDigit := false
	for i := start; i < len(data); i++ {
		b := data[i]
		if b == '\r' || b == '\n' {
			break
		}
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid digit: %c", b)
		}
		foundDigit = true
		result = result*10 + int(b-'0')
		if result < 0 {
			return 0, fmt.Errorf("number too large")
		}
	}

	if !foundDigit {
		return 0, fmt.Errorf("no digits found")
	}

	if negative {
		return -result, nil
	}
	return result, nil
}

// parseRESPArray парсит RESP массив команд
func parseRESPArray(reader *bufio.Reader, buf []byte, args *[]string) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("EOF")
		}
		if err == bufio.ErrBufferFull {
			line, err = reader.ReadBytes('\n')
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if len(line) < 3 || line[0] != RESPArray {
		return fmt.Errorf("invalid RESP array")
	}

	count, err := parseInt(line[1:])
	if err != nil {
		return fmt.Errorf("invalid array count: %v", err)
	}

	if count < 0 {
		return fmt.Errorf("negative array count")
	}

	*args = (*args)[:0]
	if cap(*args) < count {
		*args = make([]string, 0, count)
	}

	for i := 0; i < count; i++ {
		arg, err := parseRESPBulkString(reader, buf)
		if err != nil {
			return err
		}
		*args = append(*args, arg)
	}

	return nil
}

// parseRESPBulkString парсит RESP bulk string
// buf - переиспользуемый буфер для чтения данных
func parseRESPBulkString(reader *bufio.Reader, buf []byte) (string, error) {
	// Читаем длину: $N\r\n
	// ReadSlice возвращает ссылку на данные внутри буфера, без выделения памяти
	line, err := reader.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			line, err = reader.ReadBytes('\n')
			if err != nil {
				return "", err
			}
		} else {
			return "", err
		}
	}

	if len(line) < 3 || line[0] != RESPBulkString {
		return "", fmt.Errorf("invalid RESP bulk string")
	}

	length, err := parseInt(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	if length == -1 {
		return "", nil
	}

	if length < 0 {
		return "", fmt.Errorf("negative bulk string length")
	}

	// Используем переиспользуемый буфер или выделяем новый, если данных слишком много
	var data []byte
	neededSize := length + 2
	if neededSize <= len(buf) {
		// Используем переиспользуемый буфер
		data = buf[:neededSize]
	} else {
		// Для больших значений выделяем отдельный буфер
		data = make([]byte, neededSize)
	}

	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}

	if data[length] != '\r' || data[length+1] != '\n' {
		return "", fmt.Errorf("invalid bulk string terminator")
	}

	return string(data[:length]), nil
}

// writeRESPString записывает простую строку: +OK\r\n
func writeRESPString(writer *bufio.Writer, s string) error {
	writer.WriteByte(RESPString)
	writer.WriteString(s)
	writer.WriteString("\r\n")
	return nil
}

// writeRESPError записывает ошибку: -ERR message\r\n
func writeRESPError(writer *bufio.Writer, errMsg string) error {
	writer.WriteByte(RESPError)
	writer.WriteString(errMsg)
	writer.WriteString("\r\n")
	return nil
}

// writeRESPBulkString записывает bulk string: $5\r\nvalue\r\n
func writeRESPBulkString(writer *bufio.Writer, s string) error {
	writer.WriteByte(RESPBulkString)
	writer.WriteString(strconv.Itoa(len(s)))
	writer.WriteString("\r\n")
	writer.WriteString(s)
	writer.WriteString("\r\n")
	return nil
}

// writeRESPNullBulkString записывает null bulk string: $-1\r\n
func writeRESPNullBulkString(writer *bufio.Writer) error {
	writer.WriteByte(RESPBulkString)
	writer.WriteString("-1")
	writer.WriteString("\r\n")
	return nil
}
