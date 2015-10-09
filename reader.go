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
	ErrProtocolError = errors.New("protocol error")
)

/*
type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("shredis: %s", string(pe))
}
*/

type replyReader struct {
	buf *bufio.Reader
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
	p, err := r.buf.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	negate := false
	n := 0
	for _, c := range p[:len(p)-2] {
		switch c {
		case '-':
			negate = true // FIXME: allow only for first char
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			n *= 10
			n += int(c - '0')
		default:
			return 0, ErrProtocolError
		}
	}
	if negate {
		n *= -1
	}
	return n, nil
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

	p := make([]byte, n+2)
	_, err = io.ReadFull(r.buf, p)
	if err != nil {
		return nil, err
	}
	return string(p[:n]), nil
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
