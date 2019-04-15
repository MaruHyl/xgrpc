package xgrpc

import (
	"context"
	"errors"
	"sync"

	"google.golang.org/grpc"
)

var (
	ErrManagerClosed = errors.New("manager is closed")
)

type DialFunc func(ctx context.Context, addr string) (*grpc.ClientConn, error)
type ConnFunc func(addr string, dialFunc DialFunc) Conn

type managerOptions struct {
	dial     DialFunc
	connFunc ConnFunc
}

var defaultOptions = managerOptions{
	dial: func(ctx context.Context, addr string) (conn *grpc.ClientConn, e error) {
		return grpc.DialContext(ctx, addr, grpc.WithInsecure())
	},
	connFunc: NewDefaultConn,
}

type ManagerOption func(options *managerOptions)

func WithDialFunc(dialFunc DialFunc) ManagerOption {
	return func(options *managerOptions) {
		options.dial = dialFunc
	}
}

func WithConnFunc(connFunc ConnFunc) ManagerOption {
	return func(options *managerOptions) {
		options.connFunc = connFunc
	}
}

// gRpc manager: 管理地址和conn的关系
type Manager struct {
	rw      sync.RWMutex
	connMap map[string]Conn
	closed  bool
	managerOptions
}

func NewManager(opts ...ManagerOption) *Manager {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	return &Manager{
		connMap:        make(map[string]Conn),
		managerOptions: options,
	}
}

func (m *Manager) GetConn(ctx context.Context, addr string) (conn Conn, err error) {
	m.rw.RLock()
	if m.closed {
		m.rw.RUnlock()
		return nil, ErrManagerClosed
	}
	var ok bool
	conn, ok = m.connMap[addr]
	m.rw.RUnlock()

	m.rw.Lock()
	defer m.rw.Unlock()

	if !ok {
		conn, ok = m.connMap[addr]
		if !ok {
			conn = m.connFunc(addr, m.dial)
			m.connMap[addr] = conn
		}
	}

	return conn, nil
}

func (m *Manager) Close() error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if !m.closed {
		m.closed = true
		for _, c := range m.connMap {
			_ = c.Close()
		}
	}
	return nil
}
