package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anytls/sing-anytls/padding"
	singAtomic "github.com/sagernet/sing/common/atomic"
	"github.com/sagernet/sing/common/logger"
)

func TestClientReset(t *testing.T) {
	var pad singAtomic.TypedValue[*padding.PaddingFactory]
	padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &pad)

	var dialCount atomic.Int32
	var connections []*resetTestConn
	client := NewClient(context.Background(), logger.NOP(), func(context.Context) (net.Conn, error) {
		conn := newResetTestConn()
		connections = append(connections, conn)
		dialCount.Add(1)
		return conn, nil
	}, &pad, 0, 0, 0, false)
	t.Cleanup(func() { client.Close() })

	stream, err := client.CreateStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	if dialCount.Load() != 1 {
		t.Fatalf("unexpected dial count: %d", dialCount.Load())
	}

	client.Reset()
	if !connections[0].isClosed() {
		t.Fatal("pooled connection was not closed by Reset")
	}

	stream, err = client.CreateStream(context.Background())
	if err != nil {
		t.Fatalf("CreateStream after Reset: %v", err)
	}
	if dialCount.Load() != 2 {
		t.Fatalf("unexpected dial count after Reset: %d", dialCount.Load())
	}
	if connections[1].isClosed() {
		t.Fatal("new connection is closed")
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	client.Reset()
	if !connections[1].isClosed() {
		t.Fatal("new pooled connection was not closed by the second Reset")
	}
	client.Reset()
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = client.CreateStream(context.Background()); err == nil {
		t.Fatal("CreateStream succeeded after Close")
	}
}

func TestClientResetRejectsInFlightSession(t *testing.T) {
	var pad singAtomic.TypedValue[*padding.PaddingFactory]
	padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &pad)

	dialStarted := make(chan *resetTestConn, 1)
	continueDial := make(chan struct{})
	var dialCount atomic.Int32
	client := NewClient(context.Background(), logger.NOP(), func(context.Context) (net.Conn, error) {
		conn := newResetTestConn()
		if dialCount.Add(1) == 1 {
			dialStarted <- conn
			<-continueDial
		}
		return conn, nil
	}, &pad, 0, 0, 0, false)
	t.Cleanup(func() { client.Close() })

	createDone := make(chan error, 1)
	go func() {
		_, err := client.CreateStream(context.Background())
		createDone <- err
	}()
	firstConn := <-dialStarted
	client.Reset()
	close(continueDial)
	if err := <-createDone; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("CreateStream error = %v, want net.ErrClosed", err)
	}
	if !firstConn.isClosed() {
		t.Fatal("in-flight connection was not closed after Reset")
	}

	stream, err := client.CreateStream(context.Background())
	if err != nil {
		t.Fatalf("CreateStream after rejected in-flight session: %v", err)
	}
	if dialCount.Load() != 2 {
		t.Fatalf("unexpected dial count: %d", dialCount.Load())
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientDoesNotPoolStaleSession(t *testing.T) {
	var pad singAtomic.TypedValue[*padding.PaddingFactory]
	padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &pad)
	client := NewClient(context.Background(), logger.NOP(), nil, &pad, 0, 0, 0, false)
	t.Cleanup(func() { client.Close() })

	conn := newResetTestConn()
	session := NewClientSession(conn, &pad, logger.NOP())
	session.seq = 1
	session.generation = client.resetGeneration.Load()
	client.Reset()
	client.putIdleSession(session)
	if !conn.isClosed() {
		t.Fatal("stale session was not closed")
	}
	if !client.idleSession.IsEmpty() {
		t.Fatal("stale session was added to the idle pool")
	}
}

type resetTestConn struct {
	closed    chan struct{}
	closeOnce sync.Once
}

func newResetTestConn() *resetTestConn {
	return &resetTestConn{closed: make(chan struct{})}
}

func (c *resetTestConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *resetTestConn) Write(payload []byte) (int, error) {
	if c.isClosed() {
		return 0, net.ErrClosed
	}
	return len(payload), nil
}

func (c *resetTestConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	return nil
}

func (c *resetTestConn) LocalAddr() net.Addr              { return nil }
func (c *resetTestConn) RemoteAddr() net.Addr             { return nil }
func (c *resetTestConn) SetDeadline(time.Time) error      { return nil }
func (c *resetTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *resetTestConn) SetWriteDeadline(time.Time) error { return nil }

func (c *resetTestConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
