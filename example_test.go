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
	cs := []shredis.Cmd{
		shredis.Build("Key", "GET", "Key"),
		shredis.Build("Key2", "GET", "Key2"),
	}
	res := shr.Exec(cs...)
	for _, c := range res {
		if c.Err != nil {
			fmt.Printf("Command err: %s. Original command: %v\n", c.Err, c.Cmd)
			continue
		}
		fmt.Printf("Result: %s, command: %s\n", c.Res, c.Cmd)
	}
}
