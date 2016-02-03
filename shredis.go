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
	"fmt"
	"math/rand"
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
	shards    []shard
	onConnect []*Cmd
	connwg    sync.WaitGroup
	logCB     LogCB
}

// Option is an option to New.
type Option func(*Shred)

type shard struct {
	label, addr string
	conn        conn
}

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

// New starts all connections to redis daemons. `shards` is a map with
// shardname:address.
func New(shards map[string]string, options ...Option) *Shred {
	s := &Shred{
		shards: make([]shard, len(shards)),
		logCB:  func(string, int, time.Duration, error) {},
	}
	for _, o := range options {
		o(s)
	}

	var (
		bs []bucket
		i  = 0
	)
	for l, h := range shards {
		bs = append(bs, bucket{Label: l, ID: i, Weight: 1})
		s.connwg.Add(1)
		c := newConn()
		go func(l, h string) {
			c.handle(h, l, s.onConnect, s.logCB)
			s.connwg.Done()
		}(l, h)
		s.shards[i] = shard{
			conn:  c,
			label: l,
			addr:  h,
		}
		i++
	}
	s.ket = ketamaNew(bs)
	return s
}

// Close closes all connections. Blocks.
func (s *Shred) Close() {
	for _, sh := range s.shards {
		sh.conn.close()
	}
	s.connwg.Wait()
}

// Exec is the way to execute commands. It is goroutine-safe.
func (s *Shred) Exec(cs ...*Cmd) {
	var (
		wg = sync.WaitGroup{}
		ac = make([][]*Cmd, len(s.shards))
	)

	// map every action to a connection, collect all actions per connection, and
	// execute them at the same time
	for i, c := range cs {
		slot := s.ket.Slot(c.hash)
		ac[slot] = append(ac[slot], cs[i])
	}
	for i, vs := range ac {
		if len(vs) > 0 {
			wg.Add(1)
			s.shards[i].conn.exec(action{
				cmds: vs,
				wg:   &wg,
			})
		}
	}

	wg.Wait()
}

// MapExec builds a command of `fields` and sends it to every redis. It returns
// a shardname->cmd map.
func (s *Shred) MapExec(fields ...string) map[string]*Cmd {
	var (
		wg   = sync.WaitGroup{}
		cmds = map[string]*Cmd{}
	)

	for _, shard := range s.shards {
		cmd := &Cmd{
			// no key
			payload: buildCommand(fields, nil),
			err:     ErrNotExecuted,
		}
		cmds[shard.label] = cmd
		wg.Add(1)
		shard.conn.exec(action{
			cmds: []*Cmd{cmd},
			wg:   &wg,
		})
	}

	wg.Wait()

	return cmds
}

// RandExec executes the given command on a randomly picked server. It returns
// the shardname and address of the selected server.
// You need to seed the random function once.
func (s *Shred) RandExec(cmd *Cmd) (string, string) {
	var (
		wg    = sync.WaitGroup{}
		shard = s.shards[rand.Intn(len(s.shards))]
	)

	wg.Add(1)
	shard.conn.exec(action{
		cmds: []*Cmd{cmd},
		wg:   &wg,
	})
	wg.Wait()
	return shard.label, shard.addr
}

// ShardExec executes the given command on a specific server.
func (s *Shred) ShardExec(label string, cmd *Cmd) error {
	var sh *shard
	for _, si := range s.shards {
		if si.label == label {
			sh = &si
			break
		}
	}
	if sh == nil {
		return fmt.Errorf("unknown shard: %s", label)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	sh.conn.exec(action{
		cmds: []*Cmd{cmd},
		wg:   &wg,
	})
	wg.Wait()
	return nil
}

// Addr gives the address for a key. For debugging/testing.
func (s *Shred) Addr(key string) string {
	return s.shards[s.ket.Slot(hashKey(key))].label
}
