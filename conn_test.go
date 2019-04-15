package xgrpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
)

type server struct{}

func (s *server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func startServer(t *testing.T) (*grpc.Server, net.Listener) {
	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	reflection.Register(s)

	go func() {
		require.NoError(t, s.Serve(lis))
	}()

	return s, lis
}

func TestDefaultConn(t *testing.T) {
	s, lis := startServer(t)

	c := NewDefaultConn(
		lis.Addr().String(),
		func(ctx context.Context, addr string) (conn *grpc.ClientConn, e error) {
			return grpc.DialContext(ctx, addr, grpc.WithInsecure())
		})
	testDefaultConn(t, s, c)
}

func testDefaultConn(t *testing.T, s *grpc.Server, c Conn) {
	rc := c.(*defaultConn)
	require.False(t, c.IsClosed())

	testTimeout := 1 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// normal, test multi-goroutines
	t.Parallel()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		t.Run(fmt.Sprintf("conn-%d", i), func(t *testing.T) {
			defer wg.Done()
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
		})
	}
	wg.Wait()
	require.Equal(t, 0, int(rc.ref.Load()))

	// stop server
	s.GracefulStop()

	// error
	_, _, err := c.Get(ctx)
	require.EqualError(t, err, ErrConnNotReady.Error())

	// close
	require.NoError(t, c.Close())
	require.True(t, c.IsClosed())
	_, _, err = c.Get(ctx)
	require.EqualError(t, err, ErrConnClosed.Error())
}
