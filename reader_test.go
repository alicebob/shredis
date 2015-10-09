package shredis

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestReader(t *testing.T) {
	type cas struct {
		payload string
		want    interface{}
		err     error
	}
	for _, c := range []cas{
		{
			payload: "+OK\r\n",
			want:    "OK",
		},
		{
			payload: "+PONG\r\n",
			want:    "PONG",
		},
		{
			payload: "-Error message\r\n",
			want:    errors.New("Error message"),
		},
		{
			payload: ":1000\r\n",
			want:    int64(1000),
		},
		{
			payload: "$6\r\nfoobar\r\n",
			want:    []byte("foobar"),
		},
		{
			payload: "$0\r\n\r\n",
			want:    []byte(""),
		},
		{
			payload: "$-1\r\n",
			want:    nil,
		},
		{
			payload: "*0\r\n",
			want:    []interface{}{}, // or []interface{}(nil)
		},
		{
			payload: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			want:    []interface{}{[]byte("foo"), []byte("bar")},
		},
		{
			payload: "*3\r\n:1\r\n:2\r\n:3\r\n",
			want:    []interface{}{int64(1), int64(2), int64(3)},
		},
		{
			payload: "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n",
			want:    []interface{}{int64(1), int64(2), int64(3), int64(4), []byte("foobar")},
		},
		{
			payload: "*-1\r\n",
			want:    nil,
		},
	} {
		b := strings.NewReader(c.payload)
		r := newReplyReader(b)
		have, err := r.Next()
		if err != c.err {
			t.Errorf("have %v want %v; %q", err, c.err, c.payload)
			continue
		}
		if !reflect.DeepEqual(have, c.want) {
			t.Errorf("have %#v (%T) want %#v (%T); %q", have, have, c.want,
				c.want, c.payload)
		}
	}
}
