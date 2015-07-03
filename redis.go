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

// RedisInfoStat represents the [Stats] part of the 'INFO' command.
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
		s, err := c.GetString()
		if err != nil {
			return sum, err
		}

		for k, v := range parseInfo(s) {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return sum, err
			}
			fi := int(f)
			switch k {
			case "total_connections_received":
				sum.TotalConnectionsReceived += fi
			case "total_commands_processed":
				sum.TotalCommandsProcessed += fi
			case "instantaneous_ops_per_sec":
				sum.InstantaneousOpsPerSec += fi
			case "total_net_input_bytes":
				sum.TotalNetInputBytes += fi
			case "total_net_output_bytes":
				sum.TotalNetOutputBytes += fi
			case "instantaneous_input_kbps":
				sum.InstantaneousInputKbps += f
			case "instantaneous_output_kbps":
				sum.InstantaneousOutputKbps += f
			case "rejected_connections":
				sum.RejectedConnections += fi
			case "sync_full":
				sum.SyncFull += fi
			case "sync_partial_ok":
				sum.SyncPartialOk += fi
			case "sync_partial_err":
				sum.SyncPartialErr += fi
			case "expired_keys":
				sum.ExpiredKeys += fi
			case "evicted_keys":
				sum.EvictedKeys += fi
			case "keyspace_hits":
				sum.KeyspaceHits += fi
			case "keyspace_misses":
				sum.KeyspaceMisses += fi
			case "pubsub_channels":
				sum.PubsubChannels += fi
			case "pubsub_patterns":
				sum.PubsubPatterns += fi
			}
			// sum.LatestForkUsec = m["latest_fork_usec"]
		}
	}
	return sum, nil
}

// RedisInfoMemory represents the [Memory] part of the 'INFO' command.
type RedisInfoMemory struct {
	UsedMemory,
	UsedMemoryPeak,
	UsedMemoryLua int
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
		for k, v := range parseInfo(v) {
			switch k {
			case "used_memory":
				i, _ := strconv.Atoi(v)
				sum.UsedMemory += i
			case "used_memory_peak":
				i, _ := strconv.Atoi(v)
				sum.UsedMemoryPeak += i
			case "used_memory_lua":
				i, _ := strconv.Atoi(v)
				sum.UsedMemoryLua += i
			}
		}
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
