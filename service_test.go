package anytls

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/anytls/sing-anytls/padding"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

type serviceTestHandler func(context.Context, net.Conn, M.Socksaddr, M.Socksaddr, N.CloseHandlerFunc)

func (h serviceTestHandler) NewConnectionEx(ctx context.Context, conn net.Conn, source, destination M.Socksaddr, onClose N.CloseHandlerFunc) {
	h(ctx, conn, source, destination, onClose)
}

func TestFallbackOwnsFirstPacket(t *testing.T) {
	password := sha256.Sum256([]byte("password"))
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"short password", []byte("GET / HTTP/1.0\r\n\r\n")},
		{"unknown password", make([]byte, 32)},
		{"missing padding length", password[:]},
		{"short padding", append(binary.BigEndian.AppendUint16(append([]byte(nil), password[:]...), 10), 'x')},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, consume := range []bool{false, true} {
				var cached *bufio.CachedConn
				var first *buf.Buffer
				service, err := NewService(ServiceConfig{
					PaddingScheme: padding.DefaultPaddingScheme,
					Users:         []User{{Password: "password"}},
					Logger:        logger.NOP(),
					Handler: serviceTestHandler(func(context.Context, net.Conn, M.Socksaddr, M.Socksaddr, N.CloseHandlerFunc) {
						t.Fatal("unexpected authenticated connection")
					}),
					FallbackHandler: serviceTestHandler(func(_ context.Context, conn net.Conn, _, _ M.Socksaddr, _ N.CloseHandlerFunc) {
						cached = conn.(*bufio.CachedConn)
						if consume {
							// Like an asynchronous copier, take the first packet now and
							// consume it only after the service has returned.
							first = cached.ReadCached()
						}
					}),
				})
				require.NoError(t, err)
				conn, peer := net.Pipe()
				defer conn.Close()
				defer peer.Close()
				written := make(chan error, 1)
				go func() { _, err := peer.Write(test.data); written <- err }()
				require.NoError(t, service.NewConnection(context.Background(), conn, M.Socksaddr{}, nil))
				require.NoError(t, <-written)
				require.NotNil(t, cached)
				if consume {
					require.NotNil(t, first)
					require.Equal(t, test.data, first.Bytes())
					first.Release()
				}
				// Also cover teardown before the first packet was consumed.
				require.NoError(t, cached.Close())
			}
		})
	}
}

func TestServiceRejectsWithoutFallback(t *testing.T) {
	service, err := NewService(ServiceConfig{
		PaddingScheme: padding.DefaultPaddingScheme,
		Logger:        logger.NOP(),
		Handler: serviceTestHandler(func(context.Context, net.Conn, M.Socksaddr, M.Socksaddr, N.CloseHandlerFunc) {
			t.Fatal("unexpected authenticated connection")
		}),
	})
	require.NoError(t, err)
	conn, peer := net.Pipe()
	defer conn.Close()
	written := make(chan error, 1)
	go func() { _, err := peer.Write([]byte("invalid")); peer.Close(); written <- err }()
	require.ErrorContains(t, service.NewConnection(context.Background(), conn, M.Socksaddr{}, nil), "fallback disabled")
	require.NoError(t, <-written)
	closed, peer := net.Pipe()
	peer.Close()
	defer closed.Close()
	require.ErrorIs(t, service.NewConnection(context.Background(), closed, M.Socksaddr{}, nil), io.EOF)
}
