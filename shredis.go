package shredis

import (
	"sync"
)

type Shred struct {
	conns []chan<- action
}

func New(hosts map[string]string) *Shred {
	var conns []chan<- action
	for _, h := range hosts {
		conns = append(conns, handleConn(h))
	}
	return &Shred{
		conns: conns,
	}
}

func (s *Shred) Exec(cs []Cmd) []Res {
	r := make([]Res, len(cs))
	wg := sync.WaitGroup{}

	for i, c := range cs {
		wg.Add(1)
		s.conn(c.Key) <- action{
			cmd: c,
			done: func(i int) func(Res) {
				return func(res Res) {
					r[i] = res
					wg.Done()
				}
			}(i),
		}
	}

	wg.Wait()
	return r
}

func (s *Shred) conn(key []byte) chan<- action {
	// TODO: hash 'nd stuff.
	return s.conns[0]
}

type Res struct {
	Cmd Cmd
	Err error
	Res interface{}
}
