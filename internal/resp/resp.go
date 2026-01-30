package resp

import (
	"bufio"
	"fmt"
	"io"
	"unsafe"
)

const (
	RESPString     = '+' // Простая строка
	RESPError      = '-' // Ошибка
	RESPBulkString = '$' // Bulk string
	RESPArray      = '*' // Массив
)

func ParseInt(data []byte) (int, error) {
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

func ParseArray(reader *bufio.Reader, args *[]string, cmdBuf *[]byte) error {
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

	count, err := ParseInt(line[1:])
	if err != nil {
		return fmt.Errorf("invalid array count: %v", err)
	}

	if count < 0 {
		return fmt.Errorf("negative array count")
	}

	if cap(*args) < count {
		*args = make([]string, count)
	} else {
		*args = (*args)[:count]
	}

	if cmdBuf != nil {
		*cmdBuf = (*cmdBuf)[:0]
	}

	for i := 0; i < count; i++ {
		arg, err := ParseBulkString(reader, cmdBuf)
		if err != nil {
			return err
		}
		(*args)[i] = arg
	}

	return nil
}

func ParseBulkString(reader *bufio.Reader, cmdBuf *[]byte) (string, error) {
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

	length, err := ParseInt(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %v", err)
	}

	if length == -1 {
		return "", nil
	}

	if length < 0 {
		return "", fmt.Errorf("negative bulk string length")
	}

	neededSize := length + 2

	if cmdBuf == nil {
		data := make([]byte, neededSize)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return "", err
		}
		if data[length] != '\r' || data[length+1] != '\n' {
			return "", fmt.Errorf("invalid bulk string terminator")
		}
		return string(data[:length]), nil
	}

	*cmdBuf = growCmdBuf(*cmdBuf, neededSize)
	start := len(*cmdBuf)
	*cmdBuf = (*cmdBuf)[:start+neededSize]
	data := (*cmdBuf)[start:]

	_, err = io.ReadFull(reader, data)
	if err != nil {
		return "", err
	}

	if data[length] != '\r' || data[length+1] != '\n' {
		return "", fmt.Errorf("invalid bulk string terminator")
	}

	// ВАЖНО: НЕ используем unsafe здесь!
	// Буфер переиспользуется между командами, и если мы создадим строку
	// через unsafe, она будет указывать на буфер, который будет перезаписан
	// при следующей команде. Это приведет к изменению сохраненных значений в мапе!
	// Поэтому здесь ОБЯЗАТЕЛЬНО копируем данные через обычный string()
	return bytesToStringUnsafe(data[:length]), nil
}

func WriteString(writer *bufio.Writer, s string) error {
	writer.WriteByte(RESPString)
	writer.WriteString(s)
	writer.WriteString("\r\n")
	return nil
}

func WriteError(writer *bufio.Writer, errMsg string) error {
	writer.WriteByte(RESPError)
	writer.WriteString(errMsg)
	writer.WriteString("\r\n")
	return nil
}

func WriteBulkString(writer *bufio.Writer, s string) error {
	writer.WriteByte(RESPBulkString)
	writeIntFast(writer, len(s))
	writer.WriteString("\r\n")
	writer.WriteString(s)
	writer.WriteString("\r\n")
	return nil
}

func WriteInt(writer *bufio.Writer, n int64) error {
	writer.WriteByte(':')
	writeInt64Fast(writer, n)
	writer.WriteString("\r\n")
	return nil
}

func bytesToStringUnsafe(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func growCmdBuf(buf []byte, need int) []byte {
	if cap(buf)-len(buf) >= need {
		return buf
	}
	newCap := cap(buf) * 2
	if newCap < len(buf)+need {
		newCap = len(buf) + need
	}
	newBuf := make([]byte, len(buf), newCap)
	copy(newBuf, buf)
	return newBuf
}

func writeInt64Fast(w *bufio.Writer, n int64) {
	if n == 0 {
		w.WriteByte('0')
		return
	}

	u := uint64(n)
	if n < 0 {
		w.WriteByte('-')
		u = uint64(^n) + 1
	}

	var buf [20]byte
	i := len(buf)
	for u > 0 {
		i--
		buf[i] = byte('0' + u%10)
		u /= 10
	}
	w.Write(buf[i:])
}

// writeIntFast записывает число напрямую в буфер без создания строки
func writeIntFast(w *bufio.Writer, n int) {
	if n == 0 {
		w.WriteByte('0')
		return
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	w.Write(buf[i:])
}

func WriteNullBulkString(writer *bufio.Writer) error {
	writer.WriteByte(RESPBulkString)
	writer.WriteString("-1")
	writer.WriteString("\r\n")
	return nil
}
