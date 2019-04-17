package xgrpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"github.com/stretchr/testify/require"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

func TestManager(t *testing.T) {
	m := NewManager()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// build servers
	type server struct {
		s   *grpc.Server
		lis net.Listener
	}
	servers := make([]server, 3)
	for i := range servers {
		s, lis := startServer(t)
		servers[i] = server{s, lis}
	}

	var wg sync.WaitGroup

	t.Run("normal cases", func(t *testing.T) {
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for _, s := range servers {
					addr := s.lis.Addr().String()
					c, err := m.GetConn(ctx, addr)
					require.NoError(t, err)
					testConn(t, ctx, c, fmt.Sprintf("%d-%s", i, addr))
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("closed Conn cases", func(t *testing.T) {
		for _, s := range servers {
			c, err := m.GetConn(ctx, s.lis.Addr().String())
			require.NoError(t, err)
			require.NoError(t, c.Close())
		}
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for _, s := range servers {
					addr := s.lis.Addr().String()
					c, err := m.GetConn(ctx, addr)
					require.NoError(t, err)
					testConn(t, ctx, c, fmt.Sprintf("%d-%s", i, addr))
				}
			}(i)
		}
		wg.Wait()
	})

	for _, s := range servers {
		s.s.GracefulStop()
	}

	require.NoError(t, m.Close())
	_, err := m.GetConn(ctx, "")
	require.EqualError(t, err, ErrManagerClosed.Error())
}

func testConn(t *testing.T, ctx context.Context, c Conn, id string) {
	conn, closer, err := c.Get(ctx)
	require.NoError(t, err)
	cli := pb.NewGreeterClient(conn)
	resp, err := cli.SayHello(ctx, &pb.HelloRequest{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, "Hello test", resp.Message)
	t.Log(id, resp.Message)
	// close multi-times
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
}
