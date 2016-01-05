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
	reads, writes := prepareWork(100, 100)
	for i := 0; i < b.N; i++ {
		doWork(b, sh, reads, writes)
	}
}

func prepareWork(readN, writeN int) ([]*Cmd, []*Cmd) {
	var reads, writes []*Cmd
	for i := 0; i < writeN; i++ {
		key := fmt.Sprintf("key%d", i)
		writes = append(writes, Build(key, "HSET", key, "aap", "noot"))
	}
	for i := 0; i < readN; i++ {
		key := fmt.Sprintf("key%d", i)
		reads = append(reads, Build(key, "HGET", key, "aap"))
	}
	return reads, writes
}

func doWork(b *testing.B, sh *Shred, reads []*Cmd, writes []*Cmd) {
	sh.Exec(append(writes, reads...)...)
	for _, w := range writes {
		if _, err := w.Get(); err != nil {
			b.Error(err)
		}
	}
	for _, w := range reads {
		if v, err := w.GetString(); err != nil {
			b.Error(err)
		} else {
			if have, want := v, "noot"; have != want {
				b.Errorf("have %v, want %v", have, want)
			}
		}
	}
}

func BenchmarkBuilding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Build("key1", "HSET", "key1", "aap", "noot")
	}
}

func BenchmarkNoPrepare(b *testing.B) {
	sh := New(map[string]string{
		"shard0": "localhost:6379",
		"shard1": "localhost:6379",
		"shard2": "localhost:6379",
		"shard3": "localhost:6379",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmds := []*Cmd{
			Build("key", "HSET", "key", "aap", "noot"),
			Build("key", "HGET", "key", "aap"),
		}
		sh.Exec(cmds...)
		for _, c := range cmds {
			if _, err := c.Get(); err != nil {
				b.Error(err)
			}
		}
	}
}
