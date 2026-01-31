package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/VoolFI71/go-kv-store/internal/resp"
	"github.com/VoolFI71/go-kv-store/internal/storage"
	"github.com/cespare/xxhash/v2"
	"github.com/panjf2000/gnet/v2"
)

const (
	maxResponsesBeforeFlush = 4096
	maxBytesBeforeFlush     = 64 * 1024
)

type session struct {
	args        []string
	out         []byte
	responses   int
	shouldClose bool
}

type server struct {
	gnet.BuiltinEventEngine
	st         storage.Storage
	defaultTTL int64
}

func main() {
	addr := flag.String("addr", "tcp://0.0.0.0:6379", "listen address")
	pprofAddr := flag.String("pprof", "localhost:9090", "pprof server address (empty to disable)")
	gogc := flag.Int("gogc", 1000, "set GOGC for server")
	gcReset := flag.Bool("gc-reset", false, "force GC and free OS memory on startup")
	defaultTTLSeconds := flag.Int64("ttl", 15, "default TTL for keys in seconds (0 to disable)")
	flag.Parse()

	debug.SetGCPercent(*gogc)
	if *gcReset {
		runtime.GC()
		debug.FreeOSMemory()
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("Starting pprof server on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("Warning: pprof server failed to start: %v", err)
			}
		}()
	}

	srv := &server{st: storage.New(), defaultTTL: *defaultTTLSeconds}
	if err := gnet.Run(srv, *addr, gnet.WithMulticore(true)); err != nil {
		log.Fatalf("gnet run failed: %v", err)
	}
}

func (s *server) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	c.SetContext(&session{
		args: make([]string, 0, 64),
		out:  make([]byte, 0, 64*1024),
	})
	return nil, gnet.None
}

func (s *server) OnTraffic(c gnet.Conn) gnet.Action {
	sess := c.Context().(*session)

	for {
		n := c.InboundBuffered()
		if n == 0 {
			break
		}
		buf, err := c.Peek(n)
		if err != nil {
			return gnet.None
		}

		consumed, parseErr, ok := resp.ParseArrayBytes(buf, &sess.args)
		if parseErr != nil {
			sess.out = resp.AppendError(sess.out, "ERR invalid command format")
			_, _ = c.Discard(n)
			if len(sess.out) > 0 {
				_, _ = c.Write(sess.out)
				sess.out = sess.out[:0]
				sess.responses = 0
			}
			return gnet.Close
		}
		if !ok {
			break
		}

		action := s.handleCommand(sess, c)
		_, _ = c.Discard(consumed)
		sess.responses++

		if sess.shouldClose || c.InboundBuffered() == 0 || len(sess.out) >= maxBytesBeforeFlush || sess.responses >= maxResponsesBeforeFlush {
			if len(sess.out) > 0 {
				_, _ = c.Write(sess.out)
				sess.out = sess.out[:0]
				sess.responses = 0
			}
			if action == gnet.Close || sess.shouldClose {
				return gnet.Close
			}
		}
	}

	if sess.shouldClose {
		if len(sess.out) > 0 {
			_, _ = c.Write(sess.out)
			sess.out = sess.out[:0]
			sess.responses = 0
		}
		return gnet.Close
	}

	return gnet.None
}

func (s *server) handleCommand(sess *session, c gnet.Conn) gnet.Action {
	args := sess.args
	if len(args) == 0 {
		return gnet.None
	}

	command := args[0]
	if command == "QUIT" || command == "quit" || command == "EXIT" || command == "exit" {
		sess.out = resp.AppendString(sess.out, "OK")
		sess.shouldClose = true
		return gnet.None
	}

	if len(command) == 0 {
		sess.out = resp.AppendError(sess.out, "ERR empty command")
		return gnet.None
	}

	firstChar := command[0]
	if firstChar == 'S' || firstChar == 's' {
		if command == "SET" || command == "set" {
			if len(args) < 3 {
				sess.out = resp.AppendError(sess.out, "ERR wrong number of arguments for 'SET' command")
			} else {
				key := args[1]
				value := args[2]
				hash := xxhash.Sum64String(key)
				if s.defaultTTL > 0 {
					s.st.SetHashedWithTTLSeconds(hash, key, value, s.defaultTTL)
				} else {
					s.st.SetHashed(hash, key, value)
				}
				sess.out = resp.AppendString(sess.out, "OK")
			}
			return gnet.None
		}
	}

	if firstChar == 'G' || firstChar == 'g' {
		if command == "GET" || command == "get" {
			if len(args) < 2 {
				sess.out = resp.AppendError(sess.out, "ERR wrong number of arguments for 'GET' command")
			} else {
				key := args[1]
				hash := xxhash.Sum64String(key)
				value, ok := s.st.GetHashed(hash, key)
				if ok {
					sess.out = resp.AppendBulkString(sess.out, value)
				} else {
					sess.out = resp.AppendNullBulkString(sess.out)
				}
			}
			return gnet.None
		}
	}

	if firstChar == 'I' || firstChar == 'i' {
		if command == "INCR" || command == "incr" {
			if len(args) < 2 {
				sess.out = resp.AppendError(sess.out, "ERR wrong number of arguments for 'INCR' command")
			} else {
				key := args[1]
				hash := xxhash.Sum64String(key)
				value, err := s.st.IncrHashed(hash, key)
				if err != nil {
					sess.out = resp.AppendError(sess.out, err.Error())
				} else {
					if s.defaultTTL > 0 {
						_ = s.st.SetExpireHashed(hash, key, s.defaultTTL)
					}
					sess.out = resp.AppendInt(sess.out, value)
				}
			}
			return gnet.None
		}
	}

	if firstChar == 'E' || firstChar == 'e' {
		if command == "EXPIRE" || command == "expire" {
			if len(args) < 3 {
				sess.out = resp.AppendError(sess.out, "ERR wrong number of arguments for 'EXPIRE' command")
			} else {
				seconds, err := strconv.ParseInt(args[2], 10, 64)
				if err != nil {
					sess.out = resp.AppendError(sess.out, "ERR value is not an integer or out of range")
				} else {
					key := args[1]
					hash := xxhash.Sum64String(key)
					ok := s.st.SetExpireHashed(hash, key, seconds)
					if ok {
						sess.out = resp.AppendInt(sess.out, 1)
					} else {
						sess.out = resp.AppendInt(sess.out, 0)
					}
				}
			}
			return gnet.None
		}
	}

	if firstChar == 'C' || firstChar == 'c' {
		if command == "CONFIG" || command == "config" {
			if len(args) >= 2 && (args[1] == "GET" || args[1] == "get") {
				sess.out = append(sess.out, '*', '0', '\r', '\n')
			} else {
				sess.out = resp.AppendError(sess.out, "ERR wrong number of arguments for 'CONFIG' command")
			}
			return gnet.None
		}
	}

	if firstChar == 'P' || firstChar == 'p' {
		if command == "PING" || command == "ping" {
			sess.out = resp.AppendString(sess.out, "PONG")
			return gnet.None
		}
	}

	sess.out = resp.AppendError(sess.out, "ERR unknown command '"+command+"'")
	return gnet.None
}
