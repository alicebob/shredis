package shredis

import (
	"bytes"
	"testing"
)

func TestCommand(t *testing.T) {
	for _, c := range []struct{ have, want *Cmd }{
		{
			have: Build("k", "GET", "k"),
			want: &Cmd{
				key:     []byte("k"),
				payload: []byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
			},
		},
	} {
		if !bytes.Equal(c.have.payload, c.want.payload) {
			t.Errorf("have: %q, want: %q", c.have.payload, c.want.payload)
		}
		if !bytes.Equal(c.have.key, c.want.key) {
			t.Errorf("have: %v, want: %v", c.have.key, c.want.key)
		}
	}
}
