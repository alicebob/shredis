package shredis

import (
	"bytes"
	"strconv"
)

// Cmd is a redis command.
type Cmd struct {
	Key     []byte
	Payload []byte
}

// Build makes a command which will be send to the shard for 'key'. All
// redis commands work, but it's not advised to use commands which are stateful
// ('SELECT'), involve multiple servers ('MGET', 'MGET', 'RENAME'), or are not
// simple command->reply ('WATCH').
func Build(key string, fields ...string) Cmd {
	var b bytes.Buffer
	writeCommand(&b, fields)
	return Cmd{
		Key:     []byte(key),
		Payload: b.Bytes(),
	}
}

func writeCommand(b *bytes.Buffer, fields []string) {
	writeLen(b, '*', len(fields))
	for _, f := range fields {
		writeLen(b, '$', len(f))
		b.WriteString(f)
		b.WriteString("\r\n")
	}
}

func writeLen(b *bytes.Buffer, prefix rune, i int) {
	b.WriteRune(prefix)
	b.WriteString(strconv.Itoa(i))
	b.WriteString("\r\n")
}
