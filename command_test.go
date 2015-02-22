package shredis

import (
	"bytes"
	"testing"
)

func TestCommand(t *testing.T) {
	for _, c := range []struct{ have, want Cmd }{
		{
			have: Build("k", "GET", "k"),
			want: Cmd{
				Key:     []byte("k"),
				Payload: []byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
			},
		},
	} {
		if !bytes.Equal(c.have.Payload, c.want.Payload) {
			t.Errorf("have: %q, want: %q", c.have.Payload, c.want.Payload)
		}
		if !bytes.Equal(c.have.Key, c.want.Key) {
			t.Errorf("have: %v, want: %v", c.have.Key, c.want.Key)
		}
	}
}
