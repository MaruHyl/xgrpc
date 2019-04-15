package xgrpc

import (
	"context"
	"fmt"
	"github.com/MaruHyl/kit/examples/addsvc/pkg/addtransport"
	"io"
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

	// tests
	var wg sync.WaitGroup
	t.Parallel()
	for i := 0; i < 5; i++ {
		wg.Add(1)
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			defer wg.Done()
			for _, s := range servers {
				c, err := m.GetConn(ctx, s.lis.Addr().String())
				require.NoError(t, err)
				testConn(t, ctx, c)
			}
		})
	}
	wg.Wait()

	for _, s := range servers {
		s.s.GracefulStop()
	}

	require.NoError(t, m.Close())
	_, err := m.GetConn(ctx, "")
	require.EqualError(t, err, ErrManagerClosed.Error())
}

func testConn(t *testing.T, ctx context.Context, c Conn) {
	conn, closer, err := c.Get(ctx)
	require.NoError(t, err)
	cli := pb.NewGreeterClient(conn)
	resp, err := cli.SayHello(ctx, &pb.HelloRequest{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, "Hello test", resp.Message)
	t.Log(resp.Message)
	// close multi-times
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
	require.NoError(t, closer.Close())
}
