package shredis

import (
	"testing"

	"github.com/alicebob/miniredis"
)

func TestBasic(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	mr1.Set("TestKey", "Value!")

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
	})

	cs := []Cmd{
		Build("TestKey", "GET", "TestKey"),
	}
	res := shr.Exec(cs)
	if have, want := len(res), 1; have != want {
		t.Fatalf("have %v, want %v", have, want)
	}
	r1 := res[0]
	if r1.Err != nil {
		t.Fatalf("unexpected error: %v", r1.Err)
	}
	if have, want := string(r1.Res.([]byte)), "Value!"; have != want {
		t.Fatalf("have %q (%T), want %q (%T)", have, have, want, want)
	}
}
