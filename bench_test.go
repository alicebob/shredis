package shredis

import (
	"fmt"
	"testing"
)

func Benchmark(b *testing.B) {
	sh := New(map[string]string{
		"shard0": "localhost:6379",
		"shard1": "localhost:6379",
		"shard2": "localhost:6379",
		"shard3": "localhost:6379",
	})

	b.ResetTimer()
	work := prepareWork(100, 0.5)
	for i := 0; i < b.N; i++ {
		doWork(b, sh, work)
	}
}

func prepareWork(n int, write float64) []*Cmd {
	var cmds []*Cmd
	writeN := int(float64(n) * write)
	readN := n - writeN
	for i := 0; i < writeN; i++ {
		key := fmt.Sprintf("key%d", i)
		cmds = append(cmds, Build(key, "HSET", key, "aap", "noot"))
	}
	for i := 0; i < readN; i++ {
		key := fmt.Sprintf("key%d", i)
		cmds = append(cmds, Build(key, "HGET", key, "aap"))
	}
	return cmds
}

func doWork(b *testing.B, sh *Shred, work []*Cmd) {
	sh.Exec(work...)
	for _, w := range work {
		if _, err := w.Get(); err != nil {
			b.Error(err)
		}
	}
}
