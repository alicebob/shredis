package shredis

// BuildGet builds a GET command
func BuildGet(key string) *Cmd {
	return Build(key, "GET", key)
}

// BuildSet builds a SET command
func BuildSet(key, value string) *Cmd {
	return Build(key, "SET", key)
}
