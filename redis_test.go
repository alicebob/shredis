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
			have:    BuildSetNx("aap", "noot"),
			key:     "aap",
			payload: []string{"SETNX", "aap", "noot"},
		},
		{
			have:    BuildSetNxEx("aap", "noot", 7500*time.Millisecond),
			key:     "aap",
			payload: []string{"SET", "aap", "noot", "NX", "EX", "7"},
		},
		{
			have:    BuildExpire("aap", 7500*time.Millisecond),
			key:     "aap",
			payload: []string{"EXPIRE", "aap", "7"},
		},
		{
			have:    BuildTTL("aap"),
			key:     "aap",
			payload: []string{"TTL", "aap"},
		},
	} {
		want := &Cmd{
			key:     []byte(c.key),
			payload: buildCommand(c.payload, nil),
		}
		if !bytes.Equal(c.have.payload, want.payload) {
			t.Errorf("have: %q, want: %q", c.have.payload, want.payload)
		}
		if !bytes.Equal(c.have.key, want.key) {
			t.Errorf("have: %v, want: %v", c.have.key, want.key)
		}
	}
}

func TestSetNX(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
	})

	cmds := []*Cmd{
		BuildSetNx("some", "thing"),
		BuildSetNx("some", "other thing"),
		BuildGet("some"),
	}
	shr.Exec(cmds...)
	for i, c := range cmds {
		switch i {
		case 0:
			res, err := c.GetInt()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if have, want := res, 1; have != want {
				t.Fatalf("first SETNX: have %q, want %q", have, want)
			}
		case 1:
			res, err := c.GetInt()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if have, want := res, 0; have != want {
				t.Fatalf("second SETNX: have %q, want %q", have, want)
			}
		case 2:
			res, err := c.GetString()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if have, want := res, "thing"; have != want {
				t.Fatalf("GET: have %q (%T), want %q (%T)", have, have, want, want)
			}
		}
	}
	shr.Close()
}
