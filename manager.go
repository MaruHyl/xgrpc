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

// Manage a Conn for an address
type Manager struct {
	managerOptions
	rw      sync.RWMutex
	connMap map[string]Conn
	closed  bool
}

func NewManager(opts ...ManagerOption) *Manager {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	return &Manager{
		managerOptions: options,
		connMap:        make(map[string]Conn),
	}
}

func (m *Manager) GetConn(ctx context.Context, addr string) (conn Conn, err error) {
	// fast-path
	m.rw.RLock()
	if m.closed {
		m.rw.RUnlock()
		return nil, ErrManagerClosed
	}
	var ok bool
	conn, ok = m.connMap[addr]
	m.rw.RUnlock()

	if ok && !conn.IsClosed() {
		return
	}

	// slow-path
	m.rw.Lock()
	defer m.rw.Unlock()

	if m.closed {
		return nil, ErrManagerClosed
	}

	// double-check
	conn, ok = m.connMap[addr]
	if !ok || conn.IsClosed() {
		// make Conn
		conn = m.connFunc(addr, m.dial)
		m.connMap[addr] = conn
	}

	return
}

func (m *Manager) Close() error {
	m.rw.Lock()
	defer m.rw.Unlock()

	if !m.closed {
		m.closed = true
		for _, c := range m.connMap {
			_ = c.Close()
		}
		m.connMap = nil
	}
	return nil
}
