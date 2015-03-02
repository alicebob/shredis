package shredis

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

	get := Build("TestKey", "GET", "TestKey")
	shr.Exec(get)
	value, err := get.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if have, want := string(value.([]byte)), "Value!"; have != want {
		t.Fatalf("have %q (%T), want %q (%T)", have, have, want, want)
	}

	shr.Close()
}

func TestErr(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	shr := New(map[string]string{
		"shard0": m.Addr(),
	})
	defer shr.Close()

	// invalid argument count
	{
		c := Build("key", "SET", "key")
		shr.Exec(c)
		want := "shredis: ERR wrong number of arguments for 'set' command"
		_, err := c.Get()
		if have := err; have.Error() != want {
			t.Fatalf("have: %v, want: %v", have, want)
		}
	}

	// NIL reply on a GET, not an error.
	{
		g := Build("nosuch", "GET", "nosuch")
		shr.Exec(g)
		res, err := g.Get()
		if have, want := err, error(nil); have != want {
			t.Fatalf("have: %v, want :%v", have, want)
		}
		if res != nil {
			t.Fatalf("not a nil response: %#v", res)
		}
	}
}

func TestMultiple(t *testing.T) {
	// Multiple commands to the same Redis.
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	for i := 1; i < 11; i++ {
		mr1.Set(fmt.Sprintf("TestKey%d", i), fmt.Sprintf("Value: %d", i))
	}

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
	})

	var cs []*Cmd
	for i := 10; i > 0; i-- {
		key := fmt.Sprintf("TestKey%d", i)
		cs = append(cs, Build(key, "GET", key))
	}
	shr.Exec(cs...)
	for i, c := range cs {
		res, err := c.Get()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if have, want := string(res.([]byte)), fmt.Sprintf("Value: %d", 10-i); have != want {
			t.Fatalf("have %q (%T), want %q (%T)", have, have, want, want)
		}
	}
	shr.Close()
}

func TestHashed(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	mr2, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr2.Close()

	mr2.Set("aap", "Value!") // maps to shard1

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
		"shard1": mr2.Addr(),
	})
	defer shr.Close()

	get := Build("aap", "GET", "aap")
	shr.Exec(get)
	res, err := get.Get()
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if have, want := string(res.([]byte)), "Value!"; have != want {
		t.Fatalf("have %q (%T), want %q (%T)", have, have, want, want)
	}
}

func TestMany(t *testing.T) {
	addrs := map[string]string{}
	for i := 0; i < 10; i++ {
		m, err := miniredis.Run()
		if err != nil {
			t.Fatal(err)
		}
		addrs[fmt.Sprintf("shard%d", i)] = m.Addr()
		defer m.Close()
	}

	shr := New(addrs)
	defer shr.Close()

	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			var cs []*Cmd
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("Key-%d-%d", i, j)
				cs = append(cs, Build(key, "SET", key, "value for "+key))
			}
			shr.Exec(cs...)
			for _, r := range cs {
				if _, err := r.Get(); err != nil {
					t.Fatal(err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("Key-%d-%d", i, j)
			get := Build(key, "GET", key)
			shr.Exec(get)
			res, err := get.Get()
			if err != nil {
				t.Fatal(err)
			}
			if have, want := string(res.([]byte)), "value for "+key; have != want {
				t.Fatalf("have: %v, want :%v", have, want)
			}
		}
	}
}

func TestAuth(t *testing.T) {
	mr1, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr1.Close()
	mr1.Set("TestKey", "Value!")

	shr := New(map[string]string{
		"shard0": mr1.Addr(),
	},
		OptionAuth("secret!"),
	)

	get := Build("TestKey", "GET", "TestKey")
	shr.Exec(get)
	res, err := get.Get()
	if err != nil {
		// AUTH had an error, but that's irrelevant.
		t.Fatalf("unexpected error: %v", err)
	}

	mr1.RequireAuth("secret!")
	shr = New(map[string]string{
		"shard0": mr1.Addr(),
	},
		OptionAuth("secret!"),
	)
	shr.Exec(get)
	res, err = get.Get()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if have, want := string(res.([]byte)), "Value!"; have != want {
		t.Fatalf("have %q (%T), want %q (%T)", have, have, want, want)
	}

	shr.Close()
}

func TestReconnect(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	mr.Set("TestKey", "Value!")

	shr := New(map[string]string{
		"shard0": mr.Addr(),
	})
	defer shr.Close()

	mr.Close()

	get := Build("TestKey", "GET", "TestKey")
	shr.Exec(get)
	if _, err := get.Get(); err == nil {
		t.Fatalf("expected an error")
	}

	time.Sleep(10 * time.Millisecond)
	n := time.Now()
	shr.Exec(get)
	if _, err := get.Get(); err == nil {
		t.Fatalf("expected an error")
	}
	if d := time.Since(n); d > time.Millisecond {
		t.Fatalf("reply took too long: %s", d)
	}

	mr.Restart()
	time.Sleep(50 * time.Millisecond)
	shr.Exec(get)
	if _, err := get.Get(); err != nil {
		t.Fatalf("expected no error: %v", err)
	}
}

func TestBrokenClose(t *testing.T) {
	// Close() works with a broken server.
	shr := New(map[string]string{
		"shard0": "localhost:999999",
	})
	shr.Close()
}
