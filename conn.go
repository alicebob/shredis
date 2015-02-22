package shredis

import (
	"bufio"
	"net"
	"time"
)

type actionCB func(interface{}, error)

type action struct {
	cmd  Cmd
	done actionCB
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

// handle deals with all actions written to conn.
func (c conn) handle(addr string) {
	for {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			// TODO: do something sane here.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		// c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		// c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)

		if err := loopConnection(c, r, w); err == nil {
			// graceful shutdown
			conn.Close()
			break
		}
		conn.Close()
	}
}

// loopConnection will keep writing commands to the server until either `as` is
// closed or until we get any kind of error.
func loopConnection(c conn, r *bufio.Reader, w *bufio.Writer) error {
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
				w.Write(a.cmd.Payload)
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

		if err := w.Flush(); err != nil {
			for _, c := range outstanding {
				c(nil, err)
			}
			return err
		}

		var anyError error
		for _, c := range outstanding {
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
