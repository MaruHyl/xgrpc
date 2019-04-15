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

type Conn interface {
	Get(ctx context.Context) (*grpc.ClientConn, io.Closer, error)
	Close() error
	IsClosed() bool
}

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

func (c *defaultConn) Get(ctx context.Context) (*grpc.ClientConn, io.Closer, error) {
	// fast-path
	c.RLock()
	if c.closed {
		c.RUnlock()
		return nil, nil, ErrConnClosed
	}
	if c.conn != nil && c.conn.GetState() == connectivity.Ready {
		c.RUnlock()
		return c.conn, c.getRefCounter(), nil
	}
	c.RUnlock()

	// slow-path
	c.Lock()
	defer c.Unlock()

	if c.closed {
		return nil, nil, ErrConnClosed
	}

CheckState:
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
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	conn, err := c.dial(ctx, c.addr)
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
				err = c.conn.Close()
				c.conn = nil
				c.Unlock()
			}
		})
		return
	}
	return r
}

func (c *defaultConn) Close() (err error) {
	c.Lock()
	defer c.Unlock()

	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	c.closed = true
	return
}

func (c *defaultConn) IsClosed() (closed bool) {
	c.RLock()
	closed = c.closed
	c.RUnlock()
	return
}
