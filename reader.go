package shredis

import (
	"bufio"
	"io"
)

type replyReader struct {
	// r io.Reader
	b *bufio.Reader
}

func newReplyReader(r io.Reader) *replyReader {
	return &replyReader{
		b: bufio.NewReader(r),
	}
}

func (r *replyReader) Next() (interface{}, error) {
	return readReply(r.b)
}
