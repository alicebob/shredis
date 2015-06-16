// Package shredis is a sharded redis client, with the idea of being used as a
// replacement for twemproxy/nutcracker.
//
// Commands are sharded by a user-specified key, and send to single redis
// instance. Shredis handles the connection logic: you hand over the commands,
// and after execution you check the individual commands for their errors and
// values.
//
// Commands are sent in as few packets as possible ('pipelined' in redis
// speak), even when they come from multiple goroutines.
//
package shredis

import (
	"sync"
	"time"
)

const (
	// timeout is the dial, read, and write timeout.
	connTimeout = 1 * time.Second
)

// LogCB is optional callback to monitor batch performance. t is the time from
// the first write to the last receive of a batch, and is only non-zero on
// successful complete batch execution.
type LogCB func(servername string, batchSize int, t time.Duration, err error)

// Shred controls all connections. Make one with New().
type Shred struct {
	ket       continuum
	conns     map[string]conn
	addrs     map[string]string // just for debugging
	onConnect []*Cmd
	connwg    sync.WaitGroup
	logCB     LogCB
}

// Option is an option to New.
type Option func(*Shred)

// OptionAuth is an option to New. It supports the redis AUTH command.
func OptionAuth(pw string) Option {
	return func(s *Shred) {
		s.onConnect = append(s.onConnect, Build("", "AUTH", pw))
	}
}

// OptionLog is an option to New. It adds a callback which is executed once for
// each batch send to redis.
func OptionLog(l LogCB) Option {
	return func(s *Shred) {
		s.logCB = l
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
		logCB: func(string, int, time.Duration, error) {},
	}
	for _, o := range options {
		o(s)
	}

	for l, h := range hosts {
		bs = append(bs, bucket{Label: l, Weight: 1})

		s.connwg.Add(1)
		c := newConn()
		go func(h string) {
			c.handle(h, l, s.onConnect, s.logCB)
			s.connwg.Done()
		}(h)
		s.conns[l] = c
	}
	s.ket = ketamaNew(bs)
	return s
}

// Close closes all connections. Blocks.
func (s *Shred) Close() {
	for _, c := range s.conns {
		c.close()
	}
	s.connwg.Wait()
}

// Exec is the way to execute commands. It is goroutine-safe.
func (s *Shred) Exec(cs ...*Cmd) {
	var (
		wg = sync.WaitGroup{}
		ac = map[conn][]action{}
	)

	// map every action to a connection, collect all actions per connection, and
	// execute them at the same time
	for i, c := range cs {
		wg.Add(1)
		conn := s.conn(c.key)
		ac[conn] = append(ac[conn], action{
			cmd: cs[i],
			wg:  &wg,
		})
	}
	for c, vs := range ac {
		c.exec(vs)
	}

	wg.Wait()
}

// MapExec builds a command out of fields and sends it to every redis. It'll
// return the executed commands.
func (s *Shred) MapExec(fields ...string) []*Cmd {
	var (
		wg   = sync.WaitGroup{}
		cmds []*Cmd
	)

	for _, c := range s.conns {
		wg.Add(1)
		cmd := &Cmd{
			// no key
			payload: buildCommand(fields),
		}
		cmds = append(cmds, cmd)
		c.exec([]action{
			action{
				cmd: cmd,
				wg:  &wg,
			},
		})
	}

	wg.Wait()

	return cmds
}

func (s *Shred) conn(key []byte) conn {
	return s.conns[s.ket.Hash(key)]
}

// Addr gives the address for a key. For debugging/testing.
func (s *Shred) Addr(key string) string {
	return s.addrs[s.ket.Hash([]byte(key))]
}
