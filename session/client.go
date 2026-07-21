package session

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/anytls/sing-anytls/internal/skiplist"
	"github.com/anytls/sing-anytls/padding"
	"github.com/anytls/sing-anytls/util"
	"github.com/sagernet/sing/common/atomic"
	"github.com/sagernet/sing/common/logger"
)

type Client struct {
	die       context.Context
	dieCancel context.CancelFunc

	dialOut util.DialOutFunc

	sessionCounter  atomic.Uint64
	resetGeneration atomic.Uint64

	idleSession     *skiplist.SkipList[uint64, *Session]
	idleSessionLock sync.Mutex

	sessions     map[uint64]*Session
	sessionsLock sync.Mutex

	padding *atomic.TypedValue[*padding.PaddingFactory]

	idleSessionTimeout time.Duration
	minIdleSession     int
	disableReuse       bool

	logger logger.Logger
}

func NewClient(ctx context.Context, logger logger.Logger, dialOut util.DialOutFunc,
	_padding *atomic.TypedValue[*padding.PaddingFactory], idleSessionCheckInterval, idleSessionTimeout time.Duration, minIdleSession int,
	disableReuse bool,
) *Client {
	c := &Client{
		sessions:           make(map[uint64]*Session),
		dialOut:            dialOut,
		padding:            _padding,
		idleSessionTimeout: idleSessionTimeout,
		minIdleSession:     minIdleSession,
		disableReuse:       disableReuse,
		logger:             logger,
	}
	if idleSessionCheckInterval <= time.Second*5 {
		idleSessionCheckInterval = time.Second * 30
	}
	if c.idleSessionTimeout <= time.Second*5 {
		c.idleSessionTimeout = time.Second * 30
	}
	c.die, c.dieCancel = context.WithCancel(ctx)
	c.idleSession = skiplist.NewSkipList[uint64, *Session]()
	if !c.disableReuse {
		go func() {
			for {
				time.Sleep(idleSessionCheckInterval)
				c.idleCleanup()
				select {
				case <-c.die.Done():
					return
				default:
				}
			}
		}()
	}
	return c
}

func (c *Client) CreateStream(ctx context.Context) (net.Conn, error) {
	select {
	case <-c.die.Done():
		return nil, io.ErrClosedPipe
	default:
	}

	var session *Session
	var stream *Stream
	var err error

	if !c.disableReuse {
		session = c.getIdleSession()
	}
	if session == nil {
		session, err = c.createSession(ctx)
	}
	if session == nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	stream, err = session.OpenStream()
	if err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	stream.dieHook = func() {
		// If Session is not closed, put this Stream to pool
		if !session.IsClosed() {
			if c.disableReuse {
				session.Close()
				return
			}

			select {
			case <-c.die.Done():
				// Now client has been closed
				session.Close()
			default:
				c.putIdleSession(session)
			}
		}
	}

	return stream, nil
}

func (c *Client) putIdleSession(session *Session) {
	c.idleSessionLock.Lock()
	if session.generation == c.resetGeneration.Load() && !session.IsClosed() {
		session.idleSince = time.Now()
		c.idleSession.Insert(math.MaxUint64-session.seq, session)
		c.idleSessionLock.Unlock()
		return
	}
	shouldClose := !session.IsClosed()
	c.idleSessionLock.Unlock()
	if shouldClose {
		session.Close()
	}
}

func (c *Client) getIdleSession() (idle *Session) {
	c.idleSessionLock.Lock()
	if !c.idleSession.IsEmpty() {
		it := c.idleSession.Iterate()
		idle = it.Value()
		c.idleSession.Remove(it.Key())
	}
	c.idleSessionLock.Unlock()
	return
}

func (c *Client) createSession(ctx context.Context) (*Session, error) {
	generation := c.resetGeneration.Load()
	underlying, err := c.dialOut(ctx)
	if err != nil {
		return nil, err
	}

	session := NewClientSession(underlying, c.padding, c.logger)
	session.seq = c.sessionCounter.Add(1)
	session.generation = generation
	session.dieHook = func() {
		if !c.disableReuse {
			c.idleSessionLock.Lock()
			c.idleSession.Remove(math.MaxUint64 - session.seq)
			c.idleSessionLock.Unlock()
		}

		c.sessionsLock.Lock()
		delete(c.sessions, session.seq)
		c.sessionsLock.Unlock()
	}

	var clientClosed bool
	c.sessionsLock.Lock()
	select {
	case <-c.die.Done():
		clientClosed = true
	default:
	}
	reset := generation != c.resetGeneration.Load()
	if !clientClosed && !reset {
		c.sessions[session.seq] = session
	}
	c.sessionsLock.Unlock()
	if clientClosed || reset {
		session.Close()
		if clientClosed {
			return nil, io.ErrClosedPipe
		}
		return nil, net.ErrClosed
	}

	session.Run()
	return session, nil
}

func (c *Client) Close() error {
	c.dieCancel()
	c.Reset()
	return nil
}

// Reset closes all existing sessions without closing the client.
func (c *Client) Reset() {
	c.idleSessionLock.Lock()
	c.sessionsLock.Lock()
	c.resetGeneration.Add(1)
	sessionToClose := make([]*Session, 0, len(c.sessions))
	for _, session := range c.sessions {
		sessionToClose = append(sessionToClose, session)
	}
	c.sessions = make(map[uint64]*Session)
	c.idleSession.Clear()
	c.sessionsLock.Unlock()
	c.idleSessionLock.Unlock()

	for _, session := range sessionToClose {
		session.Close()
	}
}

func (c *Client) idleCleanup() {
	c.idleCleanupExpTime(time.Now().Add(-c.idleSessionTimeout))
}

func (c *Client) idleCleanupExpTime(expTime time.Time) {
	activeCount := 0
	var sessionToClose []*Session

	c.idleSessionLock.Lock()
	it := c.idleSession.Iterate()
	for it.IsNotEnd() {
		session := it.Value()
		key := it.Key()
		it.MoveToNext()

		if !session.idleSince.Before(expTime) {
			activeCount++
			continue
		}

		if activeCount < c.minIdleSession {
			session.idleSince = time.Now()
			activeCount++
			continue
		}

		sessionToClose = append(sessionToClose, session)
		c.idleSession.Remove(key)
	}
	c.idleSessionLock.Unlock()

	for _, session := range sessionToClose {
		session.Close()
	}
}
