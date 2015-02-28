package shredis_test

import (
	"fmt"
	"time"

	"github.com/alicebob/shredis"
)

func Example_1() {
	// Example which uses helper methods.
	shr := shredis.New(map[string]string{
		"shard0": "127.0.0.1:6389",
		"shard1": "127.0.0.1:6390",
		"shard2": "127.0.0.1:6391",
		"shard3": "127.0.0.1:6392",
	})
	defer shr.Close()

	// Use simple Build* helpers to build commands:
	set := shredis.BuildSetEx("key2", "Value", 10*time.Second)
	get := shredis.BuildGet("key2")

	// Send to all redises. This blocks, but never fails.
	shr.Exec(set, get)

	// Deal with the responses.
	// The SET:
	if _, err := set.Get(); err != nil {
		fmt.Printf("set error: %s\n", err)
	}
	// The GET:
	if vs, err := get.GetString(); err != nil {
		fmt.Printf("get error: %s\n", err)
	} else {
		fmt.Printf("got string: %q\n", vs)
	}
}

func Example_2() {
	// Example which does everything the hard (but flexible) way.
	shr := shredis.New(map[string]string{
		"shard0": "127.0.0.1:6389",
		"shard1": "127.0.0.1:6390",
		"shard2": "127.0.0.1:6391",
		"shard3": "127.0.0.1:6392",
	})
	defer shr.Close()

	// Build the commands by hand.
	// The first argument is the hash-key, the rest is the redis command.
	set := shredis.Build("Key2", "SET", "Key2", "Value")
	get := shredis.Build("Key2", "GET", "Key2")

	// Send to all redises. This blocks, but never fails.
	shr.Exec(set, get)

	// Deal with the responses.
	// The SET:
	if _, err := set.Get(); err != nil {
		fmt.Printf("set error: %s\n", err)
	}
	// The GET:
	v, err := get.Get()
	if err != nil {
		fmt.Printf("get error: %s", err)
	}
	if vs, ok := v.([]byte); !ok {
		fmt.Printf("expected a []byte, but got a %T\n", v)
	} else {
		fmt.Printf("got string: %q\n", string(vs))
	}
}
