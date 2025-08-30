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

// Prevent unused imports removal
var _ = net.Listen
var _ = os.Exit

var (
	store = make(map[string]entry)
	mu    sync.RWMutex
	cond  = sync.NewCond(&mu)
	subscriptions = make(map[net.Conn]map[string]struct{})
	subscribed    = make(map[net.Conn]bool)
	multiMode = make(map[net.Conn]bool)
	multiQueue = make(map[net.Conn][]queuedCmd)

)

type ValueType int

const (
	StringType ValueType = iota
	ListType
)

type entry struct {
	kind      ValueType
	strVal    string
	listVal   *list.List
	expiresAt time.Time
}

type queuedCmd struct {
	name string
	args []interface{}
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

		arr, ok := val.([]interface{})
		if !ok || len(arr) == 0 {
			writeError(conn, "unknown command")
			continue
		}

		cmd := strings.ToUpper(arr[0].(string))

		mu.Lock()
		inSubscribedMode := subscribed[conn]
		inMultiMode := multiMode[conn]
		mu.Unlock()

		if inMultiMode {
			switch cmd {
				case "EXEC":
					handleExec(conn, arr)
					continue
				case "DISCARD":
					handleDiscard(conn , arr)
					continue
				default:
					handleQueue(conn, arr)
					continue
			}
		}

		if inSubscribedMode {
			switch cmd {
				case "SUBSCRIBE":
					handleSubscribe(conn, arr)
					continue
				case "UNSUBSCRIBE":
					handleUnsubscribe(conn, arr)
					continue
				case "PING":
					writeArray(conn, []interface{}{"pong", ""})
					continue
				default:
					writeError(conn, fmt.Sprintf("can't execute '%s' in subscribed mode", strings.ToLower(cmd)))
					continue
			}
		}

		dispatchCommand(conn, arr)
	}
}


func dispatchCommand(conn net.Conn, arr []interface{}) {
	if len(arr) == 0 {
		writeError(conn, "empty command")
		return
	}
	
	cmd := strings.ToUpper(arr[0].(string))
	
	switch cmd {
	case "PING":
		if len(arr) == 2 {
			writeSimpleString(conn, arr[1].(string))
			} else {
				writeSimpleString(conn, "PONG")
			}
			
		case "ECHO":
			if len(arr) != 2 {
				writeError(conn, "wrong number of arguments for 'echo'")
				return
			}
			writeBulkString(conn, arr[1].(string))
			
		case "SET":
			handleSet(conn, arr)
			
		case "GET":
			handleGet(conn, arr)
			
		case "RPUSH":
			handlePush(conn, arr, false)
			
		case "LPUSH":
			handlePush(conn, arr, true)
			
		case "LLEN":
			handleLLen(conn, arr)
			
		case "LRANGE":
			handleLRange(conn, arr)
			
		case "LPOP":
			handleLPop(conn, arr)
			
		case "BLPOP":
			handleBLPop(conn, arr)
			
		case "SUBSCRIBE":
			handleSubscribe(conn, arr)
			
		case "PUBLISH":
			handlePublish(conn, arr)
			
		case "INCR":
			handleIncr(conn, arr)
			
		case "MULTI":
			handleMulti(conn, arr)
			
		case "EXEC":
			writeError(conn, "EXEC without MULTI")
		
		case "DISCARD":
			writeError(conn, "DISCARD without MULTI")
		default:
			writeError(conn, fmt.Sprintf("unknown command '%s'", cmd))
		}
	}
	
func handleDiscard(conn net.Conn, arr []interface{}) {
	mu.Lock()
	defer mu.Unlock()

	if !multiMode[conn] {
		writeError(conn, "DISCARD without MULTI")
		return
	}

	multiMode[conn] = false
	delete(multiQueue, conn)

	writeSimpleString(conn, "OK")
}

func handleExec(conn net.Conn, arr []interface{}) {
		
	mu.RLock()
	q, ok := multiQueue[conn]

	if !ok {
		mu.Unlock()
		writeError(conn, "EXEC without MULTI")
		return
	}
	delete(multiQueue, conn)
	multiMode[conn] = false
	mu.RUnlock()


	results := make([]interface{}, len(q))

	for i, cmd := range q {
		fullCmd := append([]interface{}{cmd.name}, cmd.args...)

		readerPipe, writerPipe := net.Pipe()

		go func() {

			defer writerPipe.Close()
			dispatchCommand(writerPipe, fullCmd)
		}()

		parsedResponse, err := ParseRESP(bufio.NewReader(readerPipe))
		if err != nil {
			results[i] = fmt.Errorf("internal error executing '%s': %v", cmd.name, err)
		} else {
			results[i] = parsedResponse
		}
	}

	writeArray(conn, results)
}

func handleQueue(conn net.Conn, arr []interface{}) {

	cmdName, _ := arr[0].(string)
	args := arr[1:]

	mu.Lock()
	multiQueue[conn] = append(multiQueue[conn], queuedCmd{
		name: cmdName,
		args: args,
	})
	mu.Unlock()

	writeSimpleString(conn, "QUEUED")
}

func handleMulti(conn net.Conn, arr []interface{}) {
	mu.Lock()
	defer mu.Unlock()

	multiMode[conn] = true

	multiQueue[conn] = []queuedCmd{}

	writeSimpleString(conn, "OK")
}

func handleIncr(conn net.Conn, arr []interface{}) {
	if len(arr) != 2 {
        writeError(conn, "wrong number of arguments for 'incr'")
        return
    }

    key, ok := arr[1].(string)
    if !ok {
        writeError(conn, "key must be a string")
        return
    }

    mu.Lock()
	defer mu.Unlock()
    e, ok := store[key]

	if !ok {
		store[key] = entry{strVal: "0", kind: StringType}
		e = store[key]
	}
	
	count, err := strconv.Atoi(e.strVal)

	if err != nil {
		writeError(conn, "value is not an integer or out of range")
		return
	}

	count++
	e.strVal = strconv.Itoa(count)
	store[key] = e

	writeInteger(conn, count)
}

func handleUnsubscribe(conn net.Conn, arr []interface{}) {
    mu.Lock()
    defer mu.Unlock()

    chans, ok := subscriptions[conn]
    if !ok {
        subscriptions[conn] = make(map[string]struct{})
        chans = subscriptions[conn]
    }

    if len(arr) == 1 {
        for channel := range chans {
            delete(chans, channel)
            count := len(chans)
            writeArray(conn, []interface{}{"unsubscribe", channel, count})
        }
        return
    }

    for i := 1; i < len(arr); i++ {
        channel, ok := arr[i].(string)
        if !ok {
            writeError(conn, "channel name must be a string")
            return
        }

        delete(chans, channel)

        count := len(chans)
        writeArray(conn, []interface{}{"unsubscribe", channel, count})
    }

    if len(chans) == 0 {
        subscribed[conn] = false
    }
}


func handlePublish(conn net.Conn, arr []interface{}) {
    if len(arr) < 3 {
        writeError(conn, "wrong number of arguments for 'publish'")
        return
    }

    channel, ok := arr[1].(string)
    if !ok {
        writeError(conn, "channel name must be a string")
        return
    }

	message, ok := arr[2].(string)
	if !ok {
		writeError(conn, "message must be a string")
	}

    mu.RLock()
	receivers := []net.Conn{}
    for c, chans := range subscriptions {
        if _, subscribed := chans[channel]; subscribed {
            receivers = append(receivers, c)
        }
    }
    mu.RUnlock()

	for _, c := range receivers {
		writeArray(c, []interface{}{"message", channel, message})
	}

    writeInteger(conn, len(receivers))
}


func handleSubscribe(conn net.Conn, arr []interface{}) {
    if len(arr) < 2 {
        writeError(conn, "wrong number of arguments for 'subscribe'")
        return
    }

	mu.Lock()
    if _, ok := subscriptions[conn]; !ok {
        subscriptions[conn] = make(map[string]struct{})
    }

	subscribed[conn] = true

    for i := 1; i < len(arr); i++ {
        channel, ok := arr[i].(string)
        if !ok {
            writeError(conn, "channel name must be a string")
            return
        }
    
        subscriptions[conn][channel] = struct{}{}

        count := len(subscriptions[conn])

        writeArray(conn, []interface{}{"subscribe", channel, count})
    }

	mu.Unlock()
}

func handleSet(conn net.Conn, arr []interface{}) {
	if len(arr) < 3 {
		writeError(conn, "wrong number of arguments for 'set'")
		return
	}
	key := arr[1].(string)
	val := arr[2].(string)
	var expires time.Time

	if len(arr) == 5 && strings.ToUpper(arr[3].(string)) == "PX" {
		ms, err := strconv.Atoi(arr[4].(string))
		if err != nil {
			writeError(conn, "PX value is not an integer")
			return
		}
		expires = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}

	mu.Lock()
	store[key] = entry{strVal: val, expiresAt: expires}
	mu.Unlock()

	writeSimpleString(conn, "OK")
}

func handleGet(conn net.Conn, arr []interface{}) {
	if len(arr) != 2 {
		writeError(conn, "wrong number of arguments for 'get'")
		return
	}
	key := arr[1].(string)

	mu.RLock()
	e, ok := store[key]
	mu.RUnlock()

	if !ok {
		writeNullBulk(conn)
		return
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		mu.Lock()
		delete(store, key)
		mu.Unlock()
		writeNullBulk(conn)
		return
	}

	writeBulkString(conn, e.strVal)
}

func handlePush(conn net.Conn, arr []interface{}, left bool) {
	if len(arr) < 3 {
		writeError(conn, fmt.Sprintf("not enough arguments for '%s'", strings.ToLower(arr[0].(string))))
		return
	}
	key := arr[1].(string)
	values := arr[2:]

	mu.Lock()
	n := pushToList(key, values, left)
	cond.Signal()
	mu.Unlock()

	writeInteger(conn, n)
}

func pushToList(key string, values []interface{}, left bool) int {
	e, ok := store[key]
	var l *list.List
	if ok {
		l = e.listVal
	} else {
		l = list.New()
	}
	for _, val := range values {
		if left {
			l.PushFront(val.(string))
		} else {
			l.PushBack(val.(string))
		}
	}
	store[key] = entry{listVal: l, kind: ListType}
	return l.Len()
}

func handleLLen(conn net.Conn, arr []interface{}) {
	if len(arr) < 2 {
		writeError(conn, "not enough arguments for 'llen'")
		return
	}
	key := arr[1].(string)

	mu.RLock()
	e, ok := store[key]
	mu.RUnlock()

	if !ok || e.listVal == nil {
		writeInteger(conn, 0)
		return
	}

	writeInteger(conn, e.listVal.Len())
}

func handleLRange(conn net.Conn, arr []interface{}) {
	if len(arr) < 4 {
		writeArray(conn, []interface{}{})
		return
	}

	key := arr[1].(string)
	start, _ := strconv.Atoi(arr[2].(string))
	stop, _ := strconv.Atoi(arr[3].(string))

	mu.RLock()
	e, ok := store[key]
	mu.RUnlock()

	if !ok || e.listVal == nil {
		writeArray(conn, []interface{}{})
		return
	}

	l := e.listVal
	length := l.Len()
	start, stop, valid := normalizeRange(start, stop, length)
	if !valid {
		writeArray(conn, []interface{}{})
		return
	}

	values := make([]interface{}, 0, stop-start+1)
	idx := 0
	for el := l.Front(); el != nil; el = el.Next() {
		if idx >= start && idx <= stop {
			values = append(values, el.Value.(string))
		}
		if idx > stop {
			break
		}
		idx++
	}

	writeArray(conn, values)
}

func handleLPop(conn net.Conn, arr []interface{}) {
	if len(arr) < 2 {
		writeError(conn, "not enough arguments for 'lpop'")
		return
	}
	key := arr[1].(string)
	count := 1

	if len(arr) >= 3 {
		num, err := strconv.Atoi(arr[2].(string))
		if err != nil || num <= 0 {
			writeError(conn, "optional argument # of elements removed must be valid integer")
			return
		}
		count = num
	}

	mu.Lock()
	e, ok := store[key]
	if !ok || e.listVal.Len() == 0 {
		mu.Unlock()
		if count == 1 {
			writeNullBulk(conn)
		} else {
			writeArray(conn, []interface{}{})
		}
		return
	}

	l := e.listVal
	removed := []interface{}{}
	for i := 0; i < count; i++ {
		front := l.Front()
		if front == nil {
			break
		}
		val := l.Remove(front).(string)
		removed = append(removed, val)
	}
	store[key] = e
	mu.Unlock()

	if count == 1 {
		writeBulkString(conn, removed[0].(string))
	} else {
		writeArray(conn, removed)
	}
}

func handleBLPop(conn net.Conn, arr []interface{}) {
	if len(arr) < 3 {
		writeError(conn, "wrong number of arguments for 'blpop'")
		return
	}

	key := arr[1].(string)
	timeoutSec, err := strconv.ParseFloat(arr[2].(string), 64)
	if err != nil || timeoutSec < 0 {
		writeError(conn, "invalid timeout")
		return
	}

	var timer *time.Timer
	var timedOut bool

	mu.Lock()
	defer mu.Unlock()

	for {
		// Check if the list has data
		e, ok := store[key]
		if ok && e.listVal != nil && e.listVal.Len() > 0 {
			l := e.listVal
			front := l.Front()
			val := l.Remove(front).(string)
			store[key] = e
			writeArray(conn, []interface{}{key, val})
			return
		}

		// Timeout = 0 means wait forever
		if timeoutSec == 0 {
			cond.Wait()
			continue
		}

		// Setup timer once
		if timer == nil {
			timer = time.AfterFunc(time.Duration(timeoutSec*float64(time.Second)), func() {
				mu.Lock()
				timedOut = true
				cond.Broadcast()
				mu.Unlock()
			})
		}

		// Wait to be woken by push or timeout
		cond.Wait()

		if timedOut {
			writeNullArray(conn)
			return
		}
	}
}

// ================= RESP helpers =================

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
			return nil, nil
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*': // Array
		line, _ := r.ReadString('\n')
		count, _ := strconv.Atoi(strings.TrimSuffix(line, "\r\n"))
		if count == -1 {
			return nil, nil
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

func normalizeRange(start, stop, length int) (int, int, bool) {
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start >= length || start > stop {
		return 0, 0, false
	}
	return start, stop, true
}

// ================= RESP Write Helpers =================

func writeSimpleString(w io.Writer, s string) {
	fmt.Fprintf(w, "+%s\r\n", s)
}

func writeBulkString(w io.Writer, s string) {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
}

func writeInteger(w io.Writer, n int) {
	fmt.Fprintf(w, ":%d\r\n", n)
}

func writeArray(conn net.Conn, arr []interface{}) {
	fmt.Fprintf(conn, "*%d\r\n", len(arr))
	for _, elem := range arr {
		switch v := elem.(type) {
		case string:
			writeBulkString(conn, v)
		case int:
			writeInteger(conn, v)
		case int64:
			writeInteger(conn, int(v))
		case []byte:
			writeBulkString(conn, string(v))
		case error:
			fmt.Fprintf(conn, "-%s\r\n", v.Error())
		case nil:
			fmt.Fprintf(conn, "$-1\r\n") 
		default:
			writeBulkString(conn, fmt.Sprintf("%v", v))
		}
	}
}

func writeNullArray(w io.Writer) {
	w.Write([]byte("*-1\r\n"))
}

func writeNullBulk(w io.Writer) {
	w.Write([]byte("$-1\r\n"))
}

func writeError(w io.Writer, msg string) {
	fmt.Fprintf(w, "-ERR %s\r\n", msg)
}
