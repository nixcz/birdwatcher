package bird

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// macOS limits Unix socket paths to 103 usable characters. t.TempDir() embeds
// the full test name which can exceed that limit for longer test names.
// tempSocketPath uses os.MkdirTemp with a short prefix to stay well under the
// limit on all platforms.

// ---------------------------------------------------------------------------
// TestSocketConn_ReadLine — unit test readLine() using net.Pipe()
// ---------------------------------------------------------------------------

func TestSocketConn_ReadLine(t *testing.T) {
	// net.Pipe() gives us a synchronous in-memory connection pair.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sc := &socketConn{
		conn:   client,
		reader: bufio.NewReader(client),
		writer: bufio.NewWriter(client),
	}

	cases := []struct {
		send        string
		wantCode    string
		wantMessage string
	}{
		{"0001 BIRD ready.\n", "0001", "BIRD ready."},
		{"1007-Route information\n", "1007", "Route information"},
		{" continuation line\n", "", "continuation line"},
		{"0000 \n", "0000", ""},
	}

	// Write all lines from the server side, then close so the reader
	// eventually gets EOF (after we've consumed all expected lines).
	go func() {
		for _, tc := range cases {
			server.Write([]byte(tc.send))
		}
	}()

	for _, tc := range cases {
		code, msg, err := sc.readLine()
		if err != nil {
			t.Fatalf("readLine() error for line %q: %v", tc.send, err)
		}
		if code != tc.wantCode {
			t.Errorf("code: got %q, want %q (input %q)", code, tc.wantCode, tc.send)
		}
		if msg != tc.wantMessage {
			t.Errorf("message: got %q, want %q (input %q)", msg, tc.wantMessage, tc.send)
		}
	}
}

func TestSocketConn_ReadLine_AsyncNotification(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	sc := &socketConn{
		conn:   client,
		reader: bufio.NewReader(client),
		writer: bufio.NewWriter(client),
	}

	go func() {
		server.Write([]byte("+async notification\n"))
	}()

	code, msg, err := sc.readLine()
	if err != nil {
		t.Fatalf("readLine() error: %v", err)
	}
	if code != "+" {
		t.Errorf("code: got %q, want %q", code, "+")
	}
	if msg != "async notification" {
		t.Errorf("message: got %q, want %q", msg, "async notification")
	}
}

// ---------------------------------------------------------------------------
// helpers — temporary Unix socket listener for pool tests
// ---------------------------------------------------------------------------

// tempSocketPath returns a short path suitable for a Unix socket, staying
// well under the 103-character macOS limit for sun_path.
func tempSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bw")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}

// mockBIRDServer starts a mock BIRD server that:
//  1. Accepts one connection.
//  2. Sends the greeting.
//  3. Calls handler(conn) for request/response logic.
//  4. Closes the connection.
//
// It returns a channel that is closed when the handler has finished.
func mockBIRDServer(t *testing.T, path string, handler func(net.Conn)) chan struct{} {
	t.Helper()

	done := make(chan struct{})
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return // listener was closed
		}
		defer conn.Close()
		conn.Write([]byte("0001 BIRD ready.\n"))
		handler(conn)
	}()

	return done
}

// ---------------------------------------------------------------------------
// TestSocketPool_RunCommand — happy-path pool test via a mock server
// ---------------------------------------------------------------------------

func TestSocketPool_RunCommand(t *testing.T) {
	path := tempSocketPath(t)

	// The mock server accepts 1 connection (poolSize=1), sends the greeting,
	// reads one command line, then replies with two lines.
	_ = mockBIRDServer(t, path, func(conn net.Conn) {
		r := bufio.NewReader(conn)
		// Read the command line sent by request()
		cmd, err := r.ReadString('\n')
		if err != nil {
			t.Errorf("mock server read command: %v", err)
			return
		}
		_ = cmd
		// Send a two-line reply
		conn.Write([]byte("2002-Route entry\n"))
		conn.Write([]byte("0000 \n"))
	})

	pool, err := newSocketPool(path, 1)
	if err != nil {
		t.Fatalf("newSocketPool: %v", err)
	}

	reader, err := pool.runCommand("protocols")
	if err != nil {
		t.Fatalf("runCommand: %v", err)
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}

	got := string(body)
	if !strings.Contains(got, "2002") {
		t.Errorf("expected response to contain '2002', got: %q", got)
	}
	if !strings.Contains(got, "0000") {
		t.Errorf("expected response to contain terminal '0000', got: %q", got)
	}
}

// ---------------------------------------------------------------------------
// TestSocketPool_Reconnect — pool reconnects on a dropped connection
// ---------------------------------------------------------------------------

func TestSocketPool_Reconnect(t *testing.T) {
	path := tempSocketPath(t)

	// Two connection handlers:
	// 1st: accepts, sends greeting, then closes immediately (simulates a drop).
	// 2nd: accepts, sends greeting, reads command, sends valid reply.
	handlers := []func(net.Conn){
		func(conn net.Conn) {
			// Accept the initial pool-setup connection: read greeting already
			// sent by the harness, then do nothing — the pool setup will use
			// this connection. We close here to force a broken pipe on the
			// first runCommand() call.
			conn.Close()
		},
		func(conn net.Conn) {
			// New connection after reconnect: answer the command normally.
			r := bufio.NewReader(conn)
			_, err := r.ReadString('\n')
			if err != nil {
				return
			}
			conn.Write([]byte("1000-Reconnected\n"))
			conn.Write([]byte("0000 \n"))
		},
	}

	// We need the listener to serve `poolSize + 1` connections total:
	// poolSize for the initial pool setup, plus 1 for the reconnect.
	// To keep the test simple we use poolSize=1.
	const poolSize = 1

	// The multiMockBIRDServer sends a greeting for EACH accepted connection.
	// But our pool setup already consumed the greeting for the first conn.
	// So we need a listener that serves pool-setup conns AND the reconnect conn.
	//
	// We build a custom listener that:
	//  - For connections 1..poolSize: just sends the greeting (pool init).
	//  - For connection poolSize+1: sends greeting + answers one command.

	done := make(chan struct{})
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		defer close(done)
		idx := 0
		for idx < len(handlers) {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Always send the greeting first
			conn.Write([]byte("0001 BIRD ready.\n"))
			handlers[idx](conn)
			idx++
		}
	}()

	pool, err := newSocketPool(path, poolSize)
	if err != nil {
		t.Fatalf("newSocketPool: %v", err)
	}

	// The first runCommand will hit a broken connection and trigger a reconnect.
	reader, err := pool.runCommand("protocols")
	if err != nil {
		t.Fatalf("runCommand after reconnect: %v", err)
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading reconnected response: %v", err)
	}

	got := string(body)
	if !strings.Contains(got, "Reconnected") {
		t.Errorf("expected 'Reconnected' in response, got: %q", got)
	}
}

// ---------------------------------------------------------------------------
// TestSocketPool_Status — verify status() output shape
// ---------------------------------------------------------------------------

func TestSocketPool_Status(t *testing.T) {
	path := tempSocketPath(t)

	const poolSize = 3

	// Spin up enough mock connections for pool initialisation.
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Write([]byte("0001 BIRD ready.\n"))
			// Keep connections open so pool init succeeds; they'll be
			// cleaned up when the test ends.
		}
	}()

	pool, err := newSocketPool(path, poolSize)
	if err != nil {
		t.Fatalf("newSocketPool: %v", err)
	}

	statuses := pool.status()
	if len(statuses) != poolSize {
		t.Fatalf("status: got %d entries, want %d", len(statuses), poolSize)
	}

	for i, s := range statuses {
		busy, ok := s["busy"].(bool)
		if !ok {
			t.Errorf("entry %d: 'busy' missing or not bool", i)
		}
		connected, ok := s["connected"].(bool)
		if !ok {
			t.Errorf("entry %d: 'connected' missing or not bool", i)
		}
		if busy {
			t.Errorf("entry %d: expected busy=false at rest, got true", i)
		}
		if !connected {
			t.Errorf("entry %d: expected connected=true after init, got false", i)
		}
	}
}

// ---------------------------------------------------------------------------
// TestNewSocketPool_FailPartial — pool teardown on partial init failure
// ---------------------------------------------------------------------------

func TestNewSocketPool_FailPartial(t *testing.T) {
	// Use a path that has no listener — all connections should fail.
	path := filepath.Join(t.TempDir(), "nonexistent.ctl")

	_, err := newSocketPool(path, 4)
	if err == nil {
		t.Fatal("expected error when socket path does not exist, got nil")
	}

	// Ensure error message is informative
	if !strings.Contains(err.Error(), path) {
		t.Errorf("error should mention the socket path %q, got: %v", path, err)
	}
}

// ---------------------------------------------------------------------------
// TestSocketPool_RunCommand_MultipleRequests — sequential reuse of connections
// ---------------------------------------------------------------------------

func TestSocketPool_RunCommand_MultipleRequests(t *testing.T) {
	path := tempSocketPath(t)

	const (
		poolSize = 1
		numReqs  = 3
	)

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		// Accept one connection for the pool, then serve numReqs commands on it.
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("0001 BIRD ready.\n"))

		r := bufio.NewReader(conn)
		for i := 0; i < numReqs; i++ {
			_, err := r.ReadString('\n')
			if err != nil {
				return
			}
			fmt.Fprintf(conn, "2002-Reply%d\n0000 \n", i)
		}
	}()

	pool, err := newSocketPool(path, poolSize)
	if err != nil {
		t.Fatalf("newSocketPool: %v", err)
	}

	for i := 0; i < numReqs; i++ {
		reader, err := pool.runCommand("status")
		if err != nil {
			t.Fatalf("request %d: runCommand error: %v", i, err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("request %d: read error: %v", i, err)
		}
		want := fmt.Sprintf("Reply%d", i)
		if !strings.Contains(string(body), want) {
			t.Errorf("request %d: expected %q in response, got: %q", i, want, string(body))
		}
	}
}
