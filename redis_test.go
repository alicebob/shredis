package shredis_test

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/alicebob/shredis"
)

func TestGet(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	mr1.Set("TestKey", "Value!")

	shr := shredis.New(map[string]string{
		"shard0": mr1.Addr(),
	})

	var (
		set = shredis.Build("foo", "SET", "foo", "bar")
		get = shredis.BuildGet("foo")
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
