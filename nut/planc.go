package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/alicebob/shredis"
)

// Get lines in this format on stdin and compares the key:
// 'A' '127.0.0.1:6382:1'

func main() {
	sh := shredis.New(map[string]string{
		"foo": "127.0.0.1:6380",
		"bar": "127.0.0.1:6381",
	})

	r := bufio.NewReader(os.Stdin)
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			break
		}
		f := strings.Fields(strings.TrimSpace(l))
		key := strings.Trim(f[0], "'")
		want := strings.Trim(f[1], "'")
		want = want[:len(want)-2]
		hashed := sh.Addr(key)
        // fmt.Printf("%q -> %q == %q?\n", key, hashed, want)
		if hashed != want {
			fmt.Printf("%q -> %q (want %q)\n", key, hashed, want)
		}
	}
}
