package xgrpc

import (
	"context"
	"errors"
	"io"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

var (
	ErrConnNotReady = errors.New("conn is not ready")
	ErrConnClosed   = errors.New("conn is closed")
)

// Conn is a interface because the default implementation is that
// there is only one connection for an address, but for some special
// cases, you may need an address with a connection pool.
type Conn interface {
	Get(ctx context.Context) (*grpc.ClientConn, io.Closer, error)
	Close() error
	IsClosed() bool
}

// Manage a gRpc-connection for an address
type defaultConn struct {
	sync.RWMutex
	addr   string
	conn   *grpc.ClientConn
	dial   DialFunc
	ref    *atomic.Int64
	closed bool
}

func NewDefaultConn(addr string, dialFunc DialFunc) Conn {
	return &defaultConn{
		addr: addr,
		dial: dialFunc,
		ref:  atomic.NewInt64(0),
	}
}

func (c *defaultConn) Get(ctx context.Context) (conn *grpc.ClientConn, closer io.Closer, err error) {
	// fast-path
	c.RLock()
	if c.closed {
		c.RUnlock()
		return nil, nil, ErrConnClosed
	}
	if c.conn != nil && c.conn.GetState() == connectivity.Ready {
		// copy before release lock
		conn, closer = c.conn, c.getRefCounter()
		c.RUnlock()
		return conn, closer, nil
	}
	c.RUnlock()

	// slow-path
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, nil, ErrConnClosed
	}

CheckState:
	// 1.Ready. Return conn & ref++
	// 2.Connecting or Idle. Wait for state change and check again,
	// if the state has not changed until ctx times out, return ErrConnNotReady
	// 3.TransientFailure or Shutdown. Goto Reconnect
	if c.conn != nil {
		for {
			s := c.conn.GetState()
			switch s {
			case connectivity.Ready:
				return c.conn, c.getRefCounter(), nil
			case connectivity.Connecting, connectivity.Idle:
				if !c.conn.WaitForStateChange(ctx, s) {
					return nil, nil, ErrConnNotReady
				}
			case connectivity.TransientFailure, connectivity.Shutdown:
				goto Reconnect
			default:
				panic("should not happen")
			}
		}
	}

Reconnect:
	// 1. close conn and set it nil.
	// 2. dial and make a new one
	// 3. if no error, goto CheckState
	_ = c.closeConnLocked()
	conn, err = c.dial(ctx, c.addr)
	if err != nil {
		return nil, nil, err
	}
	c.conn = conn
	goto CheckState
}

type refCounter func() error

func (c refCounter) Close() error {
	return c()
}

func (c *defaultConn) getRefCounter() io.Closer {
	c.ref.Inc()
	var closedOnce sync.Once
	var r refCounter = func() (err error) {
		closedOnce.Do(func() {
			ref := c.ref.Dec()
			if ref < 0 {
				panic("should not happen")
			}
			if ref == 0 {
				c.Lock()
				err = c.closeConnLocked()
				c.Unlock()
			}
		})
		return
	}
	return r
}

func (c *defaultConn) closeConnLocked() (err error) {
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	return err
}

func (c *defaultConn) Close() (err error) {
	c.Lock()
	defer c.Unlock()

	err = c.closeConnLocked()
	c.closed = true
	return
}

func (c *defaultConn) IsClosed() (closed bool) {
	c.RLock()
	closed = c.closed
	c.RUnlock()
	return
}
