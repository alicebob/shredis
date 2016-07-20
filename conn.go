package shredis

import (
	"bufio"
	"net"
	"time"
)

type conn struct {
	c net.Conn
	r *replyReader
	w *bufio.Writer
}

func newConn(addr string, onConnect []*Cmd) (*conn, error) {
	c, err := net.DialTimeout("tcp", addr, connTimeout)
	if err != nil {
		return nil, err
	}

	var (
		r = newReplyReader(c)
	)

	for _, cmd := range onConnect {
		c.SetDeadline(time.Now().Add(connTimeout))
		if _, err := c.Write(cmd.payload); err != nil {
			c.Close()
			return nil, err
		}
		if _, err := r.Next(); err != nil {
			// AUTH errors won't be flagged.
			c.Close()
			return nil, err
		}
	}
	return &conn{
		c: c,
		r: r,
		w: bufio.NewWriter(c),
	}, nil
}

func (c *conn) Close() {
	c.c.Close()
}

func (c *conn) Exec(cmds []*Cmd) error {
	c.c.SetDeadline(time.Now().Add(connTimeout))
	for _, cmd := range cmds {
		c.w.Write(cmd.payload)
	}
	if err := c.w.Flush(); err != nil {
		for _, a := range cmds {
			a.set(nil, err)
		}
		return err
	}

	for j, cmd := range cmds {
		res, err := c.r.Next()
		if err != nil {
			for _, c := range cmds[j:] {
				c.set(nil, err)
			}
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
	return nil
}
