package shredis

import (
	"fmt"
	"sync"
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
		res := shr.Exec([]Cmd{Build("key", "SET", "key")})
		if have, want := len(res), 1; have != want {
			t.Fatalf("have: %v, want :%v", have, want)
		}
		want := "ERR wrong number of arguments for 'set' command"
		if have := res[0].Err; have.Error() != want {
			t.Fatalf("have: %v, want: %v", have, want)
		}
	}

	// NIL reply on a GET, not an error.
	{
		res := shr.Exec([]Cmd{Build("nosuch", "GET", "nosuch")})
		if have, want := len(res), 1; have != want {
			t.Fatalf("have: %v, want :%v", have, want)
		}
		r0 := res[0]
		if have, want := r0.Err, error(nil); have != want {
			t.Fatalf("have: %v, want :%v", have, want)
		}
		if r0.Res != nil {
			t.Fatalf("not a nil response: %#v", res[0].Res)
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

	var cs []Cmd
	for i := 10; i > 0; i-- {
		cs = append(cs, Build("TestKey", "GET", fmt.Sprintf("TestKey%d", i)))
	}
	res := shr.Exec(cs)
	if have, want := len(res), 10; have != want {
		t.Fatalf("have %v, want %v", have, want)
	}
	for i, r := range res {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		if have, want := string(r.Res.([]byte)), fmt.Sprintf("Value: %d", 10-i); have != want {
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

	cs := []Cmd{
		Build("aap", "GET", "aap"),
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
			var cs []Cmd
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("Key-%d-%d", i, j)
				cs = append(cs, Build(key, "SET", key, "value for "+key))
			}
			res := shr.Exec(cs)
			if have, want := len(res), 10; have != want {
				t.Fatalf("have: %v, want :%v", have, want)
			}
			for _, r := range res {
				if r.Err != nil {
					t.Fatal(r.Err)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 1000; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("Key-%d-%d", i, j)
			res := shr.Exec([]Cmd{Build(key, "GET", key)})
			if have, want := len(res), 1; have != want {
				t.Fatalf("have: %v, want :%v", have, want)
			}
			t.Logf("payload: %q", res[0].Cmd.Payload)
			t.Logf("res: %#v", res)
			if have, want := string(res[0].Res.([]byte)), "value for "+key; have != want {
				t.Fatalf("have: %v, want :%v", have, want)
			}
		}
	}
}
