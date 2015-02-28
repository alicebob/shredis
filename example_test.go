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
	cs := []*shredis.Cmd{
		shredis.Build("Key", "GET", "Key"),
		shredis.Build("Key2", "GET", "Key2"),
	}
	shr.Exec(cs[0], cs[1])
	for _, c := range cs {
		res, err := c.Get()
		if err != nil {
			fmt.Printf("Command err: %s\n", err)
			continue
		}
		fmt.Printf("Result: %s\n", res)
	}
}
