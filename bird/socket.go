package bird

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

// socketConn is a single persistent connection to the BIRD control socket.
type socketConn struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	mu     sync.Mutex
	busy   bool
}

// newSocketConn opens a Unix-domain connection to the BIRD control socket at
// path, reads and discards the greeting line, and returns the ready connection.
func newSocketConn(path string) (*socketConn, error) {
	c, err := net.Dial("unix", path)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", path, err)
	}

	sc := &socketConn{
		conn:   c,
		reader: bufio.NewReader(c),
		writer: bufio.NewWriter(c),
	}

	// Read and discard the BIRD greeting line (e.g. "0001 BIRD ready.\n")
	if _, err := sc.reader.ReadString('\n'); err != nil {
		c.Close()
		return nil, fmt.Errorf("reading greeting from %s: %w", path, err)
	}

	return sc, nil
}

// readLine reads one line from the BIRD socket and splits it into a response
// code and a message body.
//
// Line formats (per the BIRD control protocol):
//
//	"NNNN-message"  — multi-line reply start/continuation with code
//	"NNNN message"  — single-line (terminal) reply
//	" message"      — continuation line (no code)
//	"+message"      — async notification
func (c *socketConn) readLine() (code string, message string, err error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", "", err
	}

	// Strip trailing CR/LF
	line = strings.TrimRight(line, "\r\n")

	if len(line) == 0 {
		return "", "", nil
	}

	switch line[0] {
	case '+':
		// Async notification
		return "+", line[1:], nil
	case ' ':
		// Continuation line
		return "", line[1:], nil
	default:
		// Must be a 4-digit response code
		if len(line) >= 5 {
			code = line[0:4]
			message = line[5:]
		} else {
			// Short line with no message body (e.g. "0000 ")
			code = strings.TrimRight(line, " ")
			message = ""
		}
		return code, message, nil
	}
}

// request sends a command to BIRD and accumulates the full response, returning
// it as an io.Reader. The caller must call pool.release() after consuming the
// reader. The connection mutex is held for the duration of the request and
// released before returning.
func (c *socketConn) request(command string) (io.Reader, error) {
	c.mu.Lock()
	c.busy = true

	// Write command
	if _, err := c.writer.WriteString(command + "\n"); err != nil {
		c.busy = false
		c.mu.Unlock()
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		c.busy = false
		c.mu.Unlock()
		return nil, err
	}

	// Accumulate response lines until an end-of-reply sentinel is seen.
	// Per the BIRD protocol, codes starting with '0', '8', or '9' are
	// end-of-reply sentinels.
	var sb strings.Builder
	for {
		code, msg, err := c.readLine()
		if err != nil {
			c.busy = false
			c.mu.Unlock()
			return nil, err
		}

		// Reconstruct line for the parser (preserve original format)
		if code == "" {
			// Continuation line
			sb.WriteString(" ")
			sb.WriteString(msg)
			sb.WriteString("\n")
		} else if code == "+" {
			// Async notification — skip
			continue
		} else {
			// Coded line: write as "CODE message\n" (space separator, matching
			// what birdc would produce on stdout)
			sb.WriteString(code)
			sb.WriteString(" ")
			sb.WriteString(msg)
			sb.WriteString("\n")

			// End-of-reply sentinel
			if len(code) > 0 && (code[0] == '0' || code[0] == '8' || code[0] == '9') {
				break
			}
		}
	}

	c.busy = false
	c.mu.Unlock()

	return strings.NewReader(sb.String()), nil
}

// close shuts down the underlying network connection.
func (c *socketConn) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// ---------------------------------------------------------------------------
// socketPool — a fixed-size pool of persistent BIRD socket connections
// ---------------------------------------------------------------------------

// socketPool holds a pool of reusable connections to the BIRD control socket.
type socketPool struct {
	path  string
	conns []*socketConn
	sem   chan struct{}
}

// newSocketPool creates a connection pool of the given size. All connections
// are opened eagerly; if any connection fails the pool is torn down and the
// error is returned.
func newSocketPool(path string, size int) (*socketPool, error) {
	if size <= 0 {
		size = 8
	}

	sem := make(chan struct{}, size)
	for i := 0; i < size; i++ {
		sem <- struct{}{}
	}

	conns := make([]*socketConn, 0, size)
	for i := 0; i < size; i++ {
		sc, err := newSocketConn(path)
		if err != nil {
			// Close all previously opened connections before returning
			for _, existing := range conns {
				existing.close()
			}
			return nil, fmt.Errorf("opening connection %d/%d to %s: %w", i+1, size, path, err)
		}
		conns = append(conns, sc)
	}

	return &socketPool{
		path:  path,
		conns: conns,
		sem:   sem,
	}, nil
}

// acquire blocks until a connection slot is available and returns the first
// free connection. The semaphore guarantees a free connection always exists
// when this call returns.
func (p *socketPool) acquire() *socketConn {
	<-p.sem
	for _, c := range p.conns {
		if !c.busy {
			return c
		}
	}
	// Unreachable if the semaphore invariant holds, but log defensively.
	log.Println("socket pool: acquire found no free connection despite semaphore — using first slot")
	return p.conns[0]
}

// release returns a slot token to the semaphore, allowing the next waiter to
// acquire a connection.
func (p *socketPool) release() {
	p.sem <- struct{}{}
}

// runCommand acquires a connection, runs the given command, and releases the
// connection. On a transient error (EOF, broken pipe) it attempts to reconnect
// and retry the command once before giving up.
func (p *socketPool) runCommand(command string) (io.Reader, error) {
	conn := p.acquire()
	defer p.release()

	reader, err := conn.request(command)
	if err != nil {
		// Attempt to reconnect and retry once
		log.Printf("socket pool: request error (%v), reconnecting to %s", err, p.path)
		conn.close()

		newConn, dialErr := newSocketConn(p.path)
		if dialErr != nil {
			return nil, fmt.Errorf("reconnect to %s failed: %w", p.path, dialErr)
		}

		// Replace the broken connection in the pool
		for i, c := range p.conns {
			if c == conn {
				p.conns[i] = newConn
				break
			}
		}
		conn = newConn

		reader, err = conn.request(command)
		if err != nil {
			return nil, fmt.Errorf("retry after reconnect failed: %w", err)
		}
	}

	return reader, nil
}

// status returns a slice describing the state of every connection slot.
// Each entry has keys "busy" (bool) and "connected" (bool).
func (p *socketPool) status() []map[string]interface{} {
	result := make([]map[string]interface{}, len(p.conns))
	for i, c := range p.conns {
		result[i] = map[string]interface{}{
			"busy":      c.busy,
			"connected": c.conn != nil,
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Package-level pool variable and exported accessors (used by birdwatcher.go)
// ---------------------------------------------------------------------------

// globalSocketPool is the singleton pool used by Run() when Backend == "socket".
var globalSocketPool *socketPool

// NewSocketPool is the exported constructor used from birdwatcher.go.
func NewSocketPool(path string, size int) (*socketPool, error) {
	return newSocketPool(path, size)
}

// SetGlobalSocketPool installs p as the package-level pool consulted by Run().
func SetGlobalSocketPool(p *socketPool) {
	globalSocketPool = p
}

// GetGlobalSocketPool returns the current package-level pool, or nil if none
// has been installed. Used by endpoints/status.go to attach pool status.
func GetGlobalSocketPool() *socketPool {
	return globalSocketPool
}

// Status is the exported wrapper around the unexported status() method,
// allowing endpoints to call it without exposing socketPool directly.
func (p *socketPool) Status() []map[string]interface{} {
	return p.status()
}
