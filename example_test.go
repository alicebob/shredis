package shredis_test

import (
	"fmt"

	"github.com/alicebob/shredis"
)

func main() {
	shr := shredis.New(map[string]string{
		"shard0": "127.0.0.1:6389",
		"shard1": "127.0.0.1:6390",
		"shard2": "127.0.0.1:6391",
		"shard3": "127.0.0.1:6392",
	})
	defer shr.Close()

	// Build the commands.
	cs := []*shredis.Cmd{
		shredis.Build("Key", "GET", "Key"),
		shredis.Build("Key2", "GET", "Key2"),
	}
	// Send to all redises. This blocks.
	shr.Exec(cs...)
	// Check the results.
	for _, c := range cs {
		res, err := c.Get()
		if err != nil {
			fmt.Printf("Command err: %s\n", err)
			continue
		}
		fmt.Printf("Result: %v\n", res)
	}

	// Or keep the commands around:
	set := shredis.BuildSet("Key2", "Value")
	get := shredis.BuildGet("Key2")
	shr.Exec(set, get)
	if _, err := set.Get(); err != nil {
		fmt.Printf("set error: %s", err)
	}

	v, err := get.Get()
	if err != nil {
		fmt.Printf("get error: %s", err)
	}
	fmt.Printf("got: %q", string(v.([]byte)))

	// Or use a Get* helper to deal with the interface{}:
	var vs string
	if vs, err = get.GetString(); err != nil {
		fmt.Printf("get error: %s", err)
	}
	fmt.Printf("got: %q", vs)

}
