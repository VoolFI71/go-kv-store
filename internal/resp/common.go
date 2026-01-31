package resp

import (
	"fmt"
	"unsafe"
)

const (
	RESPString     = '+'
	RESPError      = '-'
	RESPBulkString = '$'
	RESPArray      = '*'
)

func ParseInt(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("empty number")
	}
	neg := false
	if data[0] == '-' {
		neg = true
		data = data[1:]
		if len(data) == 0 {
			return 0, fmt.Errorf("no digits after minus")
		}
	}
	result := 0
	for i := 0; i < len(data); i++ {
		b := data[i]
		if b == '\r' || b == '\n' {
			break
		}
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid digit: %c", b)
		}
		result = result*10 + int(b-'0')
	}
	if neg {
		return -result, nil
	}
	return result, nil
}

func bytesToStringUnsafe(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
