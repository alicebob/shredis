package shredis

import (
	"bufio"
	"net"
	"sync"
	"time"
)

type action struct {
	cmds []*Cmd
	wg   *sync.WaitGroup
}

func (a action) done() {
	a.wg.Done()
}

func (a action) doneError(err error) {
	for _, cmd := range a.cmds {
		cmd.set(nil, err)
	}
	a.done()
}

type conn chan action

func newConn() conn {
	return make(conn, 5)
}

func (c conn) close() {
	close(c)
}

func (c conn) exec(a action) {
	c <- a
}

// handle deals with all actions written to conn. onConnect are commands which
// will be executed on connect. Used for authentication.
func (c conn) handle(addr, label string, onConnect []*Cmd, log LogCB) {
	// wait runs when there is a connection problem. We don't want to
	// queue requests, just error them right away.
	// The returned bool is whether things are still ok.
	wait := func(err error, t time.Duration) bool {
		timeout := time.After(t)
		for {
			select {
			case <-timeout:
				return true
			case act, ok := <-c:
				if !ok {
					return false
				}
				act.doneError(err)
			}
		}
	}

loop:
	for {
		conn, err := net.DialTimeout("tcp", addr, connTimeout)
		if err != nil {
			if !wait(err, 50*time.Millisecond) {
				break
			}
			continue
		}

		var (
			r = newReplyReader(conn)
			w = bufio.NewWriter(conn)
		)

		for _, cmd := range onConnect {
			conn.SetDeadline(time.Now().Add(connTimeout))
			if _, err := conn.Write(cmd.payload); err != nil {
				conn.Close()
				if !wait(err, 50*time.Millisecond) {
					break loop
				}
				continue loop
			}
			if _, err := r.Next(); err != nil {
				// AUTH errors won't be flagged.
				conn.Close()
				if !wait(err, 50*time.Millisecond) {
					break loop
				}
				continue loop
			}
		}

		if err := loopConnection(c, r, w, conn, label, log); err == nil {
			// graceful shutdown
			conn.Close()
			break
		}
		conn.Close()
	}
}

// loopConnection will keep writing commands to the server until either `c` is
// closed or until we get any kind of error.
func loopConnection(
	c conn,
	r *replyReader,
	w *bufio.Writer,
	tcpconn net.Conn,
	label string,
	log LogCB,
) error {
	var outstanding []action

	for {
		outstanding = outstanding[:0]
		// read at least a single action, possibly more.
		a, ok := <-c
		start := time.Now()
	loop:
		for {
			if !ok {
				// graceful shutdown
				return nil
			}
			for _, cmd := range a.cmds {
				w.Write(cmd.payload)
			}
			outstanding = append(outstanding, a)
			// see if there are more commands waiting
			select {
			case a, ok = <-c:
				// go again
			default:
				break loop
			}
		}

		tcpconn.SetDeadline(time.Now().Add(connTimeout))
		if err := w.Flush(); err != nil {
			for _, a := range outstanding {
				a.doneError(err)
			}
			log(label, len(outstanding), 0, err)
			return err
		}

		for i, a := range outstanding {
			for j, cmd := range a.cmds {
				res, err := r.Next()
				if err != nil {
					for _, c := range a.cmds[j:] {
						c.set(nil, err)
					}
					a.done()
					for _, b := range outstanding[i+1:] {
						b.doneError(err)
					}
					log(label, len(outstanding), 0, err)
					return err
				}

				// 'ERR' replies. We don't close the connection for these, but
				// we do report them as error.
				if perr, ok := res.(error); ok {
					err = perr
					res = nil
				}
				cmd.set(res, err)
			}
			a.done()
		}
		log(label, len(outstanding), time.Since(start), nil)
	}
}
