package shredis

import (
	"strconv"
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

// BuildExpire builds an EXPIRE
func BuildExpire(key string, ttl time.Duration) *Cmd {
	return Build(key, "EXPIRE", key, strconv.Itoa(int(ttl.Seconds())))
}

// BuildDel is shorthand for Build(key, "DEL", key)
func BuildDel(key string) *Cmd {
	return Build(key, "DEL", key)
}

// BuildHset is shorthand for Build(key, "HSET", key, field, value)
func BuildHset(key, field, value string) *Cmd {
	return Build(key, "HSET", key, field, value)
}
