package shredis

import (
	"bytes"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
)

func TestGet(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	mr1.Set("TestKey", "Value!")

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
	})

	var (
		set = Build("foo", "SET", "foo", "bar")
		get = BuildGet("foo")
	)
	shr.Exec(set, get)
	if _, err := set.Get(); err != nil {
		t.Errorf("SET() gave an error")
	}

	v, err := get.GetString()
	if err != nil {
		t.Errorf("GET() gave an error")
	}
	if have, want := v, "bar"; have != want {
		t.Errorf("have %q, want %q", have, want)
	}
}

func TestBuilds(t *testing.T) {
	for _, c := range []struct {
		have    *Cmd
		key     string
		payload []string
	}{
		{
			have:    BuildGet("foo"),
			key:     "foo",
			payload: []string{"GET", "foo"},
		},
		{
			have:    BuildSet("aap", "noot"),
			key:     "aap",
			payload: []string{"SET", "aap", "noot"},
		},
		{
			have:    BuildSetEx("aap", "noot", 7500*time.Millisecond),
			key:     "aap",
			payload: []string{"SET", "aap", "noot", "EX", "7"},
		},
		{
			have:    BuildExpire("aap", 7500*time.Millisecond),
			key:     "aap",
			payload: []string{"EXPIRE", "aap", "7"},
		},
	} {
		var b bytes.Buffer
		writeCommand(&b, c.payload)
		want := &Cmd{
			key:     []byte(c.key),
			payload: b.Bytes(),
		}
		if !bytes.Equal(c.have.payload, want.payload) {
			t.Errorf("have: %q, want: %q", c.have.payload, want.payload)
		}
		if !bytes.Equal(c.have.key, want.key) {
			t.Errorf("have: %v, want: %v", c.have.key, want.key)
		}
	}
}
