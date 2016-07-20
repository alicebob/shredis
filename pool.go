package shredis

import (
	"errors"
)

type pool struct {
	addr      string
	conns     chan *conn
	onConnect []*Cmd
}

func newPool(addr string, size int, onConnect []*Cmd) *pool {
	cs := make(chan *conn, size)
	for i := 0; i < size; i++ {
		cs <- nil
	}
	return &pool{
		addr:      addr,
		conns:     cs,
		onConnect: onConnect,
	}
}

// Close blocks until all connections are done.
func (p *pool) Close() {
	for i := 0; i < cap(p.conns); i++ {
		if c := <-p.conns; c != nil {
			c.c.Close()
		}
	}
	close(p.conns)
}

func (p *pool) Get() (*conn, error) {
	c, ok := <-p.conns
	if !ok {
		return nil, errors.New("closed")
	}
	if c == nil {
		var err error
		c, err = newConn(p.addr, p.onConnect)
		if err != nil {
			p.conns <- nil
			return nil, err
		}
	}
	return c, nil
}

func (p *pool) Put(c *conn) {
	p.conns <- c
}
