// Package shredis is a sharded redis client, with the idea of being used as a
// replacement for twemproxy/nutcracker.
//
// Commands are shared by key, and shredis handles the connection logic: you
// hand over the commands, and you'll get the replies in the same order,
// regardless of which server the command was sent to. Commands are sent in as
// few packets as possible ('pipelined' in redis speak), even when they come
// from multiple goroutines.
//
package shredis

import (
	"sync"
	"time"
)

const (
	// connTimeout is the dial, read, and write timeout.
	connTimeout = 50 * time.Millisecond
)

// Shred controls all connections. Make one with New().
type Shred struct {
	ket       continuum
	conns     map[string]conn
	addrs     map[string]string // just for debugging
	onConnect []Cmd
}

// Option is an option to New.
type Option func(*Shred)

// OptionAuth is an option to New. It supports the redis AUTH command.
func OptionAuth(pw string) Option {
	return func(s *Shred) {
		s.onConnect = append(s.onConnect, Build("", "AUTH", pw))
	}
}

// New starts all connections to redis daemons.
func New(hosts map[string]string, options ...Option) *Shred {
	var (
		bs []bucket
	)
	s := &Shred{
		conns: map[string]conn{},
		addrs: hosts,
	}
	for _, o := range options {
		o(s)
	}

	for l, h := range hosts {
		bs = append(bs, bucket{Label: l, Weight: 1})
		c := newConn()
		go c.handle(h, s.onConnect)
		s.conns[l] = c
	}
	s.ket = ketamaNew(bs)
	return s
}

// Close asks all connections to close.
func (s *Shred) Close() {
	for _, c := range s.conns {
		c.close()
	}
}

// Exec is the way to execute commands. It is goroutinesafe. Result elements
// are always in the same order as the commands.
func (s *Shred) Exec(cs ...Cmd) []Res {
	var (
		r  = make([]Res, len(cs))
		wg = sync.WaitGroup{}
		ac = map[conn][]action{}
	)

	// map every action to a connection, collect all actions per connection, and
	// execute them at the same time
	for i, c := range cs {
		r[i].Cmd = c
		wg.Add(1)
		conn := s.conn(c.Key)
		ac[conn] = append(ac[conn], action{
			cmd: c,
			done: func(i int) actionCB {
				return func(res interface{}, err error) {
					r[i].Res = res
					r[i].Err = err
					wg.Done()
				}
			}(i),
		})
	}
	for c, vs := range ac {
		c.exec(vs)
	}

	wg.Wait()
	return r
}

func (s *Shred) conn(key []byte) conn {
	return s.conns[s.ket.Hash(string(key))]
}

// Addr gives the address for a key. For debugging/testing.
func (s *Shred) Addr(key string) string {
	return s.addrs[s.ket.Hash(key)]
}

// Res are the elements returned by Exec().
type Res struct {
	Cmd Cmd
	Err error
	Res interface{}
}
