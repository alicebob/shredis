package shredis

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

type action struct {
	cmd *Cmd
	wg  *sync.WaitGroup
}

func (a action) Done(res interface{}, err error) {
	a.cmd.res = res
	a.cmd.err = nil
	if err != nil {
		a.cmd.err = fmt.Errorf("shredis: %s", err)
	}
	a.wg.Done()
}

type conn chan []action

func newConn() conn {
	return make(conn, 10)
}

func (c conn) close() {
	close(c)
}

func (c conn) exec(a []action) {
	c <- a
}

// handle deals with all actions written to conn. onConnect are commands which
// will be executed on connect. Used for authentication.
func (c conn) handle(addr string, onConnect []*Cmd) {
	// wait runs when there is a connection problem. We don't want to
	// queue requests, just error them right away.
	// The returned bool is whether things are still ok.
	wait := func(err error, t time.Duration) bool {
		timeout := time.After(t)
		for {
			select {
			case <-timeout:
				return true
			case cmds, ok := <-c:
				if !ok {
					return false
				}
				for _, cmd := range cmds {
					cmd.Done(nil, err)
				}
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
			r = bufio.NewReader(conn)
			w = bufio.NewWriter(conn)
		)

		for _, cmd := range onConnect {
			conn.SetWriteDeadline(time.Now().Add(connTimeout))
			if _, err := conn.Write(cmd.payload); err != nil {
				conn.Close()
				if !wait(err, 50*time.Millisecond) {
					break loop
				}
				continue loop
			}
			conn.SetReadDeadline(time.Now().Add(connTimeout))
			if _, err := readReply(r); err != nil {
				// AUTH errors won't be flagged.
				conn.Close()
				if !wait(err, 50*time.Millisecond) {
					break loop
				}
				continue loop
			}
		}

		if err := loopConnection(c, r, w, conn); err == nil {
			// graceful shutdown
			conn.Close()
			break
		}
		conn.Close()
	}
}

// loopConnection will keep writing commands to the server until either `c` is
// closed or until we get any kind of error.
func loopConnection(c conn, r *bufio.Reader, w *bufio.Writer, tcpconn net.Conn) error {
	var outstanding []action

	for {
		outstanding = outstanding[:0]
		// read at least a single command, possibly more.
		as, ok := <-c
	loop:
		for {
			if !ok {
				// graceful shutdown
				return nil
			}
			for _, a := range as {
				w.Write(a.cmd.payload)
				outstanding = append(outstanding, a)
			}
			// see if there are more commands waiting
			select {
			case as, ok = <-c:
				// go again
			default:
				break loop
			}
		}

		tcpconn.SetWriteDeadline(time.Now().Add(connTimeout))
		if err := w.Flush(); err != nil {
			for _, a := range outstanding {
				a.Done(nil, err)
			}
			return err
		}

		var anyError error
		for _, a := range outstanding {
			tcpconn.SetReadDeadline(time.Now().Add(connTimeout))
			res, err := readReply(r)
			if err != nil {
				anyError = err
			} else {
				// 'ERR' replies. We don't close the connection for these, but
				// we do report them as error.
				if perr, ok := res.(error); ok {
					err = perr
					res = nil
				}
			}
			a.Done(res, err)
		}
		if anyError != nil {
			return anyError
		}
	}
}
