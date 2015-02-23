// Package shredis is a sharded redis client, with the idea of being used as a
// replacement for twemproxy/nutcracker.
//
// Commands are shared by key, and shredis handles the connection logic: you
// hand over the commands, and you'll get the replies in the same order,
// regardless which the server the command was sent to. Commands are send in as
// few packets as possible ('pipelined' in redis speak), even when then come
// from multiple goroutines.
//
package shredis

import (
	"sync"
)

// Shred controls all connections. Make one with New().
type Shred struct {
	ket   continuum
	conns map[string]conn
	addrs map[string]string // just for debugging
}

// New starts all connections to redis daemons.
func New(hosts map[string]string) *Shred {
	var (
		bs    []bucket
		conns = map[string]conn{}
	)
	for l, h := range hosts {
		bs = append(bs, bucket{Label: l, Weight: 1})
		c := newConn()
		go c.handle(h)
		conns[l] = c
	}
	return &Shred{
		ket:   ketamaNew(bs),
		conns: conns,
		addrs: hosts,
	}
}

// Close asks all connections to close.
func (s *Shred) Close() {
	for _, c := range s.conns {
		c.close()
	}
}

// Exec is the way to execute commands. It is goroutinesafe. Result elements
// are always in the same order as the commands.
func (s *Shred) Exec(cs []Cmd) []Res {
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
	// fmt.Printf("'%q' '%s'\n", key, s.ket.Hash(string(key)))
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
