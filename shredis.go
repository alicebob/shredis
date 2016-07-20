// Package shredis is a sharded redis client, with the idea of being used as a
// replacement for twemproxy/nutcracker.
//
// Commands are sharded by a user-specified key, and send to single redis
// instance. Shredis handles the connection logic: you hand over the commands,
// and after execution you check the individual commands for their errors and
// values.
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

	// connections per shard
	connsPerShard = 3
)

// LogCB is optional callback to monitor batch performance. t is the time from
// the first write to the last receive of a batch, and is only non-zero on
// successful complete batch execution.
type LogCB func(shardlabel string, batchSize int, t time.Duration, err error)

// Shred controls all connections. Make one with New().
type Shred struct {
	ket       continuum
	shards    []shard
	onConnect []*Cmd
	logCB     LogCB
	connCount int
}

// Option is an option to New.
type Option func(*Shred)

type shard struct {
	label, addr string
	pool        *pool
}

// OptionConns is an option to New. It sets the number of connections per shard (default 3).
func OptionConns(n int) Option {
	return func(s *Shred) {
		s.connCount = n
	}
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

// New makes a shred. `shards` is a map with shardlabel:address. Shard labels need to be constant, but address are allowed to change over time.
func New(shards map[string]string, options ...Option) *Shred {
	s := &Shred{
		shards:    make([]shard, len(shards)),
		logCB:     func(string, int, time.Duration, error) {},
		connCount: connsPerShard,
	}
	for _, o := range options {
		o(s)
	}

	var (
		bs []bucket
		i  = 0
	)
	for l, addr := range shards {
		bs = append(bs, bucket{Label: l, ID: i, Weight: 1})
		s.shards[i] = shard{
			label: l,
			addr:  addr,
			pool:  newPool(addr, s.connCount, s.onConnect),
		}
		i++
	}
	s.ket = ketamaNew(bs)
	return s
}

// Close closes all connections. Blocks.
func (s *Shred) Close() {
	for _, s := range s.shards {
		s.pool.Close()
	}
}

// Exec is the way to execute commands. It is goroutine-safe and blocks until all commands have executed (or have timed out).
func (s *Shred) Exec(cmds ...*Cmd) {
	if len(cmds) == 0 {
		return
	}

	perSlot := make([][]*Cmd, len(s.shards))
	for _, c := range cmds {
		slot := s.ket.Slot(c.hash)
		perSlot[slot] = append(perSlot[slot], c)
	}

	var wg = sync.WaitGroup{}
	for i, cs := range perSlot {
		if len(cs) == 0 {
			continue
		}
		wg.Add(1)
		go func(slot int, cmds []*Cmd) {
			s.shards[slot].exec(cmds, s.logCB)
			defer wg.Done()
		}(i, cs)
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

	for i := range s.shards {
		cmd := &Cmd{
			// no need for a key
			payload: buildCommand(fields, nil),
			err:     ErrNotExecuted,
		}
		wg.Add(1)
		go func(shrd *shard, cmd *Cmd) {
			shrd.exec([]*Cmd{cmd}, s.logCB)
			wg.Done()
		}(&s.shards[i], cmd)
		cmds[s.shards[i].label] = cmd
	}

	wg.Wait()
	return cmds
}

// RandExec executes the given command on a randomly picked server. It returns
// the shardname and address of the selected server.
// You need to seed the random function once.
func (s *Shred) RandExec(cmd *Cmd) (string, string) {
	var (
		slot  = rand.Intn(len(s.shards))
		shard = s.shards[slot]
	)

	shard.exec([]*Cmd{cmd}, s.logCB)
	return shard.label, shard.addr
}

// ShardExec executes the given command on a specific server.
func (s *Shred) ShardExec(label string, cmd *Cmd) error {
	var shrd *shard
	for i := range s.shards {
		if s.shards[i].label == label {
			shrd = &s.shards[i]
			break
		}
	}
	if shrd == nil {
		return fmt.Errorf("unknown shard: %s", label)
	}
	shrd.exec([]*Cmd{cmd}, s.logCB)
	return nil
}

// Addr gives the address for a key. For debugging/testing.
func (s *Shred) Addr(key string) string {
	return s.shards[s.ket.Slot(hashKey(key))].label
}

func (s *shard) exec(cmds []*Cmd, log LogCB) {
	var (
		start = time.Now()
	)
	c, err := s.pool.Get()
	if err != nil {
		for _, c := range cmds {
			c.set(nil, err)
		}
		return
	}

	if err = c.Exec(cmds); err != nil {
		c.Close()
		c = nil
	}
	s.pool.Put(c)

	if log != nil {
		var dt time.Duration
		if err == nil {
			dt = time.Since(start)
		}
		log(s.label, len(cmds), dt, err)
	}
}
