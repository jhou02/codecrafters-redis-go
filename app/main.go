package main

import (
	"bufio"
	"container/list"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1
var _ = net.Listen
var _ = os.Exit

var (
	store = make(map[string]entry)
	mu sync.RWMutex
)

type ValueType int
const (
	StringType ValueType = iota
	ListType
)

type entry struct {
	kind ValueType
	strVal string
	listVal *list.List
	expiresAt time.Time
}

func main() {
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on :6379")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		val, err := ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("Parse error:", err)
			return
		}

		// We expect commands to be arrays like ["PING"]
		arr, ok := val.([]interface{})
		if !ok || len(arr) == 0 {
			conn.Write([]byte("-ERR unknown command\r\n"))
			continue
		}

		// Convert first element to string (the command)
		cmd, _ := arr[0].(string)
		cmd = strings.ToUpper(cmd)

		switch cmd {
			case "PING":
				if len(arr) == 2 {
					conn.Write([]byte("+" + arr[1].(string) + "\r\n"))
				} else {
					conn.Write([]byte("+PONG\r\n"))
				}

			case "ECHO":
				if len(arr) != 2 {
					conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
					continue
				}
				msg, _ := arr[1].(string)
				// RESP Bulk String reply
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)))
			case "SET":
				if len(arr) < 3 {
					conn.Write([]byte("-ERR wrong number of arguments for 'set'\r\n"))
					continue
				}
				key := arr[1].(string)
				val := arr[2].(string)

				var expires time.Time

				if len(arr) == 5 && strings.ToUpper(arr[3].(string)) == "PX" {
					ms, err := strconv.Atoi(arr[4].(string))
					if err != nil {
						conn.Write([]byte("-ERR PX value is not an integer\r\n"))
						continue
					}
					expires = time.Now().Add(time.Duration(ms) * time.Millisecond)
				}

				mu.Lock()
				store[key] = entry{strVal: val, expiresAt: expires}
				mu.Unlock()

				conn.Write([]byte("+OK\r\n"))
			case "GET":
				if len(arr) != 2 {
					conn.Write([]byte("-ERR wrong number of arguments for 'get'\r\n"))
					continue
				}
				key := arr[1].(string)

				mu.RLock()
				e, ok := store[key]
				mu.RUnlock()

				if !ok {
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
					mu.Lock()
					delete(store, key)
					mu.Unlock()
					conn.Write([]byte("$-1\r\n"))
					continue
				}

				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(e.strVal), e.strVal)))
			case "RPUSH":
				if len(arr) < 2 {
					conn.Write([]byte("-ERR not enough arguments for 'rpush'\r\n"))
				}

				key := arr[1].(string)
				val := arr[2].(string)

				e, ok := store[key]

				var l *list.List
				if ok {
					l = e.listVal
				} else {
					l = list.New()
				}

				l.PushBack(val)

				mu.Lock()
				store[key] = entry{listVal: l, kind: ListType}
				mu.Unlock()
				
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", l.Len())))
			default:
				conn.Write([]byte("-ERR unknown command '" + cmd + "'\r\n"))
		}
	}
}

func ParseRESP(r *bufio.Reader) (interface{}, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch prefix {
		case '+': // Simple String
			line, _ := r.ReadString('\n')
			return strings.TrimSuffix(line, "\r\n"), nil

		case '-': // Error
			line, _ := r.ReadString('\n')
			return fmt.Errorf("%s", strings.TrimSuffix(line, "\r\n")), nil

		case ':': // Integer
			line, _ := r.ReadString('\n')
			num, _ := strconv.ParseInt(strings.TrimSuffix(line, "\r\n"), 10, 64)
			return num, nil

		case '$': // Bulk String
			line, _ := r.ReadString('\n')
			length, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
			if length == -1 {
				return nil, nil // Null bulk string
			}
			buf := make([]byte, length+2) // include \r\n
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			return string(buf[:length]), nil

		case '*': // Array
			line, _ := r.ReadString('\n')
			count, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
			if count == -1 {
				return nil, nil // Null array
			}
			arr := make([]interface{}, count)
			for i := 0; i < count; i++ {
				val, err := ParseRESP(r)
				if err != nil {
					return nil, err
				}
				arr[i] = val
			}
			return arr, nil
		}

	return nil, fmt.Errorf("unknown RESP prefix")
}
