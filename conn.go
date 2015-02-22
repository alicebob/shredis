package shredis

import (
	"bufio"
	"net"
)

type action struct {
	cmd  Cmd
	done func(Res)
}

func handleConn(addr string) chan<- action {
	c := make(chan action)
	go func() {
		conn, err := net.Dial("tcp", addr)
		defer conn.Close()
		// conn.(net.TCPConn).SetNoDelay(true) // it's the default
		r := bufio.NewReader(conn)
		for a := range c {
			// TODO: pipeline/batch things.
			if err != nil {
				res := Res{
					Cmd: a.cmd,
					Err: err,
				}
				a.done(res)
				continue
			}
			// c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			conn.Write(a.cmd.Payload)
			// c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			res := Res{
				Cmd: a.cmd,
			}
			res.Res, res.Err = readReply(r)
			a.done(res)
		}
	}()
	return c
}
