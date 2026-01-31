package resp

import (
	"bytes"
	"fmt"
)

func ParseArrayBytes(buf []byte, args *[]string) (int, error, bool) {
	if len(buf) == 0 {
		return 0, nil, false
	}
	if buf[0] != RESPArray {
		return 0, fmt.Errorf("invalid RESP array"), true
	}

	lineEnd := bytes.IndexByte(buf, '\n')
	if lineEnd == -1 {
		return 0, nil, false
	}

	count, err := ParseInt(buf[1:lineEnd])
	if err != nil {
		return 0, err, true
	}
	if count < 0 {
		return 0, fmt.Errorf("negative array count"), true
	}

	if cap(*args) < count {
		*args = make([]string, count)
	}
	*args = (*args)[:count]

	idx := lineEnd + 1
	for i := 0; i < count; i++ {
		if idx >= len(buf) {
			return 0, nil, false
		}
		if buf[idx] != RESPBulkString {
			return 0, fmt.Errorf("invalid RESP bulk string"), true
		}
		relativeLF := bytes.IndexByte(buf[idx:], '\n')
		if relativeLF == -1 {
			return 0, nil, false
		}
		lineEnd = idx + relativeLF
		length, err := ParseInt(buf[idx+1 : lineEnd])
		if err != nil {
			return 0, err, true
		}
		idx = lineEnd + 1
		if length == -1 {
			(*args)[i] = ""
			continue
		}
		if length < 0 {
			return 0, fmt.Errorf("negative bulk string length"), true
		}
		if len(buf) < idx+length+2 {
			return 0, nil, false
		}
		if buf[idx+length] != '\r' || buf[idx+length+1] != '\n' {
			return 0, fmt.Errorf("invalid bulk string terminator"), true
		}
		(*args)[i] = bytesToStringUnsafe(buf[idx : idx+length])
		idx += length + 2
	}

	return idx, nil, true
}

func AppendString(buf []byte, s string) []byte {
	buf = append(buf, RESPString)
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

func AppendError(buf []byte, msg string) []byte {
	buf = append(buf, RESPError)
	buf = append(buf, msg...)
	buf = append(buf, '\r', '\n')
	return buf
}

func AppendBulkString(buf []byte, s string) []byte {
	buf = append(buf, RESPBulkString)
	buf = appendInt(buf, int64(len(s)))
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

func AppendNullBulkString(buf []byte) []byte {
	buf = append(buf, RESPBulkString, '-', '1', '\r', '\n')
	return buf
}

func AppendInt(buf []byte, n int64) []byte {
	buf = append(buf, ':')
	buf = appendInt(buf, n)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendInt(buf []byte, n int64) []byte {
	if n >= 0 && n < int64(len(intCache)) {
		return append(buf, intCache[n]...)
	}
	u := uint64(n)
	if n < 0 {
		buf = append(buf, '-')
		u = uint64(^n) + 1
	}
	var tmp [20]byte
	i := len(tmp)
	for u > 0 {
		i--
		tmp[i] = byte('0' + u%10)
		u /= 10
	}
	return append(buf, tmp[i:]...)
}

var intCache = func() [][]byte {
	const max = 1024
	cache := make([][]byte, max+1)
	for i := 0; i <= max; i++ {
		cache[i] = []byte(fmt.Sprintf("%d", i))
	}
	return cache
}()
