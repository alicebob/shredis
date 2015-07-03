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

// BuildSetNx builds a SETNX command
func BuildSetNx(key, value string) *Cmd {
	return Build(key, "SETNX", key, value)
}

// BuildSetNX is an alias for BuildSetNx
func BuildSetNX(key, value string) *Cmd {
	return BuildSetNx(key, value)
}

// BuildSetNxEx builds a SET with EX command
func BuildSetNxEx(key, value string, ttl time.Duration) *Cmd {
	return Build(key, "SET", key, value, "NX", "EX", strconv.Itoa(int(ttl.Seconds())))
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
	TotalNetInputBytes,
	TotalNetOutputBytes,
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
	InstantaneousInputKbps,
	InstantaneousOutputKbps float64
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
			v, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return sum, err
			}
			vi := int(v)
			switch fields[0] {
			case "total_connections_received":
				sum.TotalConnectionsReceived += vi
			case "total_commands_processed":
				sum.TotalCommandsProcessed += vi
			case "instantaneous_ops_per_sec":
				sum.InstantaneousOpsPerSec += vi
			case "total_net_input_bytes":
				sum.TotalNetInputBytes += vi
			case "total_net_output_bytes":
				sum.TotalNetOutputBytes += vi
			case "instantaneous_input_kbps":
				sum.InstantaneousInputKbps += v
			case "instantaneous_output_kbps":
				sum.InstantaneousOutputKbps += v
			case "rejected_connections":
				sum.RejectedConnections += vi
			case "sync_full":
				sum.SyncFull += vi
			case "sync_partial_ok":
				sum.SyncPartialOk += vi
			case "sync_partial_err":
				sum.SyncPartialErr += vi
			case "expired_keys":
				sum.ExpiredKeys += vi
			case "evicted_keys":
				sum.EvictedKeys += vi
			case "keyspace_hits":
				sum.KeyspaceHits += vi
			case "keyspace_misses":
				sum.KeyspaceMisses += vi
			case "pubsub_channels":
				sum.PubsubChannels += vi
			case "pubsub_patterns":
				sum.PubsubPatterns += vi
			}
			// sum.LatestForkUsec = m["latest_fork_usec"]
		}
	}
	return sum, nil
}
