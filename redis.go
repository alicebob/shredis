package shredis

import (
	"strconv"
	"time"
)

// BuildGet builds a GET command
func BuildGet(key string) *Cmd {
	return Build(key, "GET", key)
}

// BuildSet builds a SET command
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
