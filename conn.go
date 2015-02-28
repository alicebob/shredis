package shredis

import (
	"bufio"
	"net"
	"time"
)

type actionCB func(interface{}, error)

type action struct {
	payload []byte
	done    actionCB
}

type conn chan []action

func newConn() conn {
	return conn(make(chan []action, 10))
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
	wait := func(err error, t time.Duration) {
		timeout := time.After(t)
		for {
			select {
			case <-timeout:
				return
			case cmds := <-c:
				for _, cmd := range cmds {
					cmd.done(nil, err)
				}
			}
		}
	}

loop:
	for {
		conn, err := net.DialTimeout("tcp", addr, connTimeout)
		if err != nil {
			wait(err, 50*time.Millisecond)
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
				wait(err, 50*time.Millisecond)
				continue loop
			}
			conn.SetReadDeadline(time.Now().Add(connTimeout))
			if _, err := readReply(r); err != nil {
				// AUTH errors won't be flagged.
				conn.Close()
				wait(err, 50*time.Millisecond)
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
	var outstanding []actionCB

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
				w.Write(a.payload)
				outstanding = append(outstanding, a.done)
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
			for _, c := range outstanding {
				c(nil, err)
			}
			return err
		}

		var anyError error
		for _, c := range outstanding {
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
			c(res, err)
		}
		if anyError != nil {
			return anyError
		}
	}
}
