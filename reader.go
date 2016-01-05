package shredis

// All replies are build of the types:
//   - string
//   - int
//   - error
//   - interface{} arrays of the above types

import (
	"bufio"
	"errors"
	"io"
)

var (
	// ErrProtocolError is returned on unexpected replies.
	ErrProtocolError = errors.New("shredis: protocol error")
)

type replyReader struct {
	buf     *bufio.Reader
	scratch []byte
}

func newReplyReader(r io.Reader) *replyReader {
	return &replyReader{
		buf: bufio.NewReader(r),
	}
}

func (r *replyReader) Next() (interface{}, error) {
	c, err := r.buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch c {
	case '+':
		return r.simpleString()
	case ':':
		return r.integer()
	case '$':
		return r.bulk()
	case '-':
		return r.error()
	case '*':
		return r.array()
	default:
		return nil, ErrProtocolError
	}
}

func (r *replyReader) readString() (string, error) {
	p, err := r.buf.ReadSlice('\n')
	if err != nil {
		return "", err
	}
	return string(p[:len(p)-2]), nil
}

func (r *replyReader) readInt() (int, error) {
	var (
		negate = false
		n      = 0
	)
	for i := 0; ; i++ {
		c, err := r.buf.ReadByte()
		if err != nil {
			return 0, err
		}
		switch {

		case c >= '0' && c <= '9':
			n = n*10 + int(c-'0')

		case i == 0 && c == '-':
			negate = true

		case c == '\r':
			r.buf.ReadByte() // flush the \n
			if negate {
				n *= -1
			}
			return n, nil

		default:
			return 0, ErrProtocolError

		}
	}
}

func (r *replyReader) simpleString() (string, error) {
	return r.readString()
}

func (r *replyReader) integer() (int, error) {
	return r.readInt()
}

func (r *replyReader) error() (error, error) {
	s, err := r.readString()
	if err != nil {
		return nil, err
	}
	return errors.New(s), nil
}

func (r *replyReader) bulk() (interface{}, error) {
	n, err := r.readInt()
	if err != nil || n < 0 {
		return nil, err
	}

	if len(r.scratch) < n+2 {
		r.scratch = make([]byte, n+2)
	}
	_, err = io.ReadFull(r.buf, r.scratch[:n+2])
	if err != nil {
		return nil, err
	}
	return string(r.scratch[:n]), nil
}

func (r *replyReader) array() (interface{}, error) {
	n, err := r.readInt()
	if err != nil || n < 0 {
		return nil, err
	}
	res := make([]interface{}, n)
	for i := range res {
		res[i], err = r.Next()
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
