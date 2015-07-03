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

// RedisInfoStats represents the [Stats] part of the 'INFO' command.
type RedisInfoStats struct {
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

// ParseInfoStats parses a 'INFO STATS' string.
func ParseInfoStats(s string) (RedisInfoStats, error) {
	r := RedisInfoStats{}
	for k, v := range parseInfo(s) {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return r, err
		}
		fi := int(f)
		switch k {
		case "total_connections_received":
			r.TotalConnectionsReceived = fi
		case "total_commands_processed":
			r.TotalCommandsProcessed = fi
		case "instantaneous_ops_per_sec":
			r.InstantaneousOpsPerSec = fi
		case "total_net_input_bytes":
			r.TotalNetInputBytes = fi
		case "total_net_output_bytes":
			r.TotalNetOutputBytes = fi
		case "instantaneous_input_kbps":
			r.InstantaneousInputKbps = f
		case "instantaneous_output_kbps":
			r.InstantaneousOutputKbps = f
		case "rejected_connections":
			r.RejectedConnections = fi
		case "sync_full":
			r.SyncFull = fi
		case "sync_partial_ok":
			r.SyncPartialOk = fi
		case "sync_partial_err":
			r.SyncPartialErr = fi
		case "expired_keys":
			r.ExpiredKeys = fi
		case "evicted_keys":
			r.EvictedKeys = fi
		case "keyspace_hits":
			r.KeyspaceHits = fi
		case "keyspace_misses":
			r.KeyspaceMisses = fi
		case "pubsub_channels":
			r.PubsubChannels = fi
		case "pubsub_patterns":
			r.PubsubPatterns = fi
		}
	}
	return r, nil
}

// ExecInfoStats calls 'INFO STATS' on every configured server and returns the
// sum.  Or error, if any server gives an error.
func ExecInfoStats(s *Shred) (RedisInfoStats, error) {
	var sum RedisInfoStats
	cmds := s.MapExec("INFO", "STATS")
	for _, c := range cmds {
		s, err := c.GetString()
		if err != nil {
			return sum, err
		}
		r, err := ParseInfoStats(s)
		if err != nil {
			return sum, err
		}
		sum.TotalConnectionsReceived += r.TotalConnectionsReceived
		sum.TotalCommandsProcessed += r.TotalCommandsProcessed
		sum.InstantaneousOpsPerSec += r.InstantaneousOpsPerSec
		sum.TotalNetInputBytes += r.TotalNetInputBytes
		sum.TotalNetOutputBytes += r.TotalNetOutputBytes
		sum.InstantaneousInputKbps += r.InstantaneousInputKbps
		sum.InstantaneousOutputKbps += r.InstantaneousOutputKbps
		sum.RejectedConnections += r.RejectedConnections
		sum.SyncFull += r.SyncFull
		sum.SyncPartialOk += r.SyncPartialOk
		sum.SyncPartialErr += r.SyncPartialErr
		sum.ExpiredKeys += r.ExpiredKeys
		sum.EvictedKeys += r.EvictedKeys
		sum.KeyspaceHits += r.KeyspaceHits
		sum.KeyspaceMisses += r.KeyspaceMisses
		sum.PubsubChannels += r.PubsubChannels
		sum.PubsubPatterns += r.PubsubPatterns
	}
	return sum, nil
}

// RedisInfoMemory represents the [Memory] part of the 'INFO' command.
type RedisInfoMemory struct {
	UsedMemory,
	UsedMemoryPeak,
	UsedMemoryLua int
}

// ParseInfoMemory parses a 'INFO MEMORY' string.
func ParseInfoMemory(s string) (RedisInfoMemory, error) {
	r := RedisInfoMemory{}
	for k, v := range parseInfo(s) {
		switch k {
		case "used_memory":
			i, err := strconv.Atoi(v)
			if err != nil {
				return r, err
			}
			r.UsedMemory = i
		case "used_memory_peak":
			i, err := strconv.Atoi(v)
			if err != nil {
				return r, err
			}
			r.UsedMemoryPeak = i
		case "used_memory_lua":
			i, err := strconv.Atoi(v)
			if err != nil {
				return r, err
			}
			r.UsedMemoryLua = i
		}
	}
	return r, nil
}

// ExecInfoMemory calls 'INFO MEMORY' on every configured server and returns the
// sum.
func ExecInfoMemory(s *Shred) (RedisInfoMemory, error) {
	var sum RedisInfoMemory
	cmds := s.MapExec("INFO", "MEMORY")
	for _, c := range cmds {
		v, err := c.GetString()
		if err != nil {
			return sum, err
		}
		r, err := ParseInfoMemory(v)
		if err != nil {
			return sum, err
		}
		sum.UsedMemory += r.UsedMemory
		sum.UsedMemoryPeak += r.UsedMemoryPeak
		sum.UsedMemoryLua += r.UsedMemoryLua
	}
	return sum, nil
}

// parse strings returned from INFO commands.
func parseInfo(s string) map[string]string {
	r := map[string]string{}
	// s is a string with lines, with 'key:somevalue\n' lines. And comments.
	for _, line := range strings.Split(s, "\r\n") {
		fields := strings.SplitN(line, ":", 2)
		if len(fields) != 2 {
			continue
		}
		r[fields[0]] = fields[1]
	}
	return r
}
