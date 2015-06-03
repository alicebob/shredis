package shredis

import (
	"strconv"
	"strings"
	"time"
)

// BuildGet is shorthand for Build(key, "GET", key)
func BuildGet(key string) *Cmd {
	return Build(key, "GET", key)
}

// BuildSet is shorthand for Build(key, "SET", key, value)
func BuildSet(key, value string) *Cmd {
	return Build(key, "SET", key, value)
}

// BuildSetEx builds a SET with EX command
func BuildSetEx(key, value string, ttl time.Duration) *Cmd {
	return Build(key, "SET", key, value, "EX", strconv.Itoa(int(ttl.Seconds())))
}

// BuildSetNX builds a SETNX command
func BuildSetNX(key, value string) *Cmd {
	return Build(key, "SETNX", key, value)
}

// BuildExpire builds an EXPIRE
func BuildExpire(key string, ttl time.Duration) *Cmd {
	return Build(key, "EXPIRE", key, strconv.Itoa(int(ttl.Seconds())))
}

// BuildDel is shorthand for Build(key, "DEL", key)
func BuildDel(key string) *Cmd {
	return Build(key, "DEL", key)
}

// BuildHget is shorthand for Build(key, "HGET", key, field)
func BuildHget(key, field string) *Cmd {
	return Build(key, "HGET", key, field)
}

// BuildHset is shorthand for Build(key, "HSET", key, field, value)
func BuildHset(key, field, value string) *Cmd {
	return Build(key, "HSET", key, field, value)
}

// RedisInfoStat is returned by ExecInfoStat. It's the [Stats] part of the
// 'INFO' command.
type RedisInfoStat struct {
	TotalConnectionsReceived,
	TotalCommandsProcessed,
	InstantaneousOpsPerSec,
	RejectedConnections,
	SyncFull,
	SyncPartialOk,
	SyncPartialErr,
	ExpiredKeys,
	EvictedKeys,
	KeyspaceHits,
	KeyspaceMisses,
	PubsubChannels,
	PubsubPatterns int
	// LatestForkUsec int
}

// ExecInfoStats calls 'INFO STATS' on every configured server and returns the
// sum.  Or error, if any server gives an error.
func ExecInfoStats(s *Shred) (RedisInfoStat, error) {
	var sum RedisInfoStat
	cmds := s.MapExec("INFO", "STATS")
	for _, c := range cmds {
		v, err := c.GetString()
		if err != nil {
			return sum, err
		}

		// v is a string with lines, with 'key:intvalue\n' lines. And comments.
		for _, line := range strings.Split(v, "\r\n") {
			fields := strings.SplitN(line, ":", 2)
			if len(fields) != 2 {
				continue
			}
			v, err := strconv.Atoi(fields[1])
			if err != nil {
				return sum, err
			}
			switch fields[0] {
			case "total_connections_received":
				sum.TotalConnectionsReceived += v
			case "total_commands_processed":
				sum.TotalCommandsProcessed += v
			case "instantaneous_ops_per_sec":
				sum.InstantaneousOpsPerSec += v
			case "rejected_connections":
				sum.RejectedConnections += v
			case "sync_full":
				sum.SyncFull += v
			case "sync_partial_ok":
				sum.SyncPartialOk += v
			case "sync_partial_err":
				sum.SyncPartialErr += v
			case "expired_keys":
				sum.ExpiredKeys += v
			case "evicted_keys":
				sum.EvictedKeys += v
			case "keyspace_hits":
				sum.KeyspaceHits += v
			case "keyspace_misses":
				sum.KeyspaceMisses += v
			case "pubsub_channels":
				sum.PubsubChannels += v
			case "pubsub_patterns":
				sum.PubsubPatterns += v
			}
			// sum.LatestForkUsec = m["latest_fork_usec"]
		}
	}
	return sum, nil
}
