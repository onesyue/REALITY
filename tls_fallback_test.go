//go:build integration

package reality_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	reality "github.com/xtls/reality"
)

// Keep the existing exported capability interface source-compatible.
var _ reality.CloseWriteConn = (*net.TCPConn)(nil)

func fallbackTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	server, err := listener.AcceptTCP()
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	for _, conn := range []*net.TCPConn{client, server} {
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
	}
	return client, server
}

type fallbackResult struct {
	conn *reality.Conn
	err  error
}
type readFailureConn struct {
	*net.TCPConn
	err error
}

func (c *readFailureConn) Read([]byte) (int, error) { return 0, c.err }

func startFallbackServer(t *testing.T, conn net.Conn, target net.Conn) <-chan fallbackResult {
	t.Helper()
	result := make(chan fallbackResult, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		defer func() {
			if p := recover(); p != nil {
				result <- fallbackResult{err: fmt.Errorf("server panic: %v", p)}
			}
		}()
		c, err := reality.Server(context.Background(), conn, &reality.Config{
			DialContext: func(context.Context, string, string) (net.Conn, error) { return target, nil },
			Type:        "tcp", Dest: "local-test", ServerNames: map[string]bool{"example.invalid": true},
		})
		result <- fallbackResult{conn: c, err: err}
	}()
	t.Cleanup(func() {
		conn.Close()
		target.Close()
		select {
		case <-joined:
		case <-time.After(2 * time.Second):
			t.Error("Server goroutine did not exit after owned connections closed")
		}
	})
	return result
}

func awaitFallbackFailure(t *testing.T, result <-chan fallbackResult) error {
	t.Helper()
	select {
	case got := <-result:
		if got.conn != nil || got.err == nil {
			t.Fatalf("unauthenticated connection returned (%v, %v)", got.conn, got.err)
		}
		if strings.Contains(got.err.Error(), "server panic:") {
			t.Fatal(got.err)
		}
		return got.err
	case <-time.After(time.Second):
		t.Fatal("Server did not release its opposite copy after terminal connection event")
		return nil
	}
}

func TestRealityClientHelloReadError(t *testing.T) {
	_, raw := fallbackTCPPair(t)
	target, _ := fallbackTCPPair(t)
	want := errors.New("injected ClientHello read failure")
	err := awaitFallbackFailure(t, startFallbackServer(t, &readFailureConn{TCPConn: raw, err: want}, target))
	if !strings.Contains(err.Error(), "failed to read client hello: "+want.Error()) {
		t.Fatalf("read cause lost: %v", err)
	}
}

func TestRealityFallbackTermination(t *testing.T) {
	for _, mode := range []string{"download_fin", "download_reset", "upload_reset", "without_close_write"} {
		t.Run(mode, func(t *testing.T) {
			client, raw := fallbackTCPPair(t)
			target, dest := fallbackTCPPair(t)
			var conn net.Conn = raw
			if mode == "without_close_write" {
				conn = struct{ net.Conn }{raw}
			}
			result := startFallbackServer(t, conn, target)
			if _, err := client.Write([]byte("GET /")); err != nil {
				t.Fatal(err)
			}
			initial := make([]byte, 5)
			if _, err := io.ReadFull(dest, initial); err != nil {
				select {
				case got := <-result:
					t.Fatalf("server ended before fallback forwarding: %v", got.err)
				default:
					t.Fatal(err)
				}
			}
			if string(initial) != "GET /" {
				t.Fatalf("fallback upload = %q", initial)
			}
			if _, err := dest.Write([]byte("reply")); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, 5)
			if _, err := io.ReadFull(client, reply); err != nil {
				t.Fatal(err)
			}
			if string(reply) != "reply" {
				t.Fatalf("fallback download = %q", reply)
			}
			switch mode {
			case "download_reset":
				if err := dest.SetLinger(0); err != nil {
					t.Fatal(err)
				}
				dest.Close()
			case "upload_reset":
				if err := client.SetLinger(0); err != nil {
					t.Fatal(err)
				}
				client.Close()
			default:
				if err := dest.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				if data, err := io.ReadAll(client); err != nil || len(data) != 0 {
					t.Fatalf("download completion = %q, %v", data, err)
				}
				if mode == "download_fin" {
					select {
					case got := <-result:
						t.Fatalf("clean download FIN closed reverse upload: %v", got.err)
					default:
					}
					if _, err := client.Write([]byte("after-fin")); err != nil {
						t.Fatal(err)
					}
					late := make([]byte, 9)
					if _, err := io.ReadFull(dest, late); err != nil {
						t.Fatal(err)
					}
					if string(late) != "after-fin" {
						t.Fatalf("reverse upload = %q", late)
					}
					if err := client.CloseWrite(); err != nil {
						t.Fatal(err)
					}
					if data, err := io.ReadAll(dest); err != nil || len(data) != 0 {
						t.Fatalf("upload completion = %q, %v", data, err)
					}
				}
			}
			awaitFallbackFailure(t, result)
		})
	}
}
