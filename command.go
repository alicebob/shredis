package shredis

import (
	"bytes"
	"fmt"
	"strconv"
)

// Cmd is a redis command.
type Cmd struct {
	key     []byte
	payload []byte
	res     interface{}
	err     error
}

// Build makes a command which will be send to the shard for 'key'. All
// redis commands work, but it's not advised to use commands which are stateful
// ('SELECT'), involve multiple servers ('MGET', 'MGET', 'RENAME'), or are not
// simple command->reply ('WATCH').
func Build(key string, fields ...string) *Cmd {
	var b bytes.Buffer
	writeCommand(&b, fields)
	return &Cmd{
		key:     []byte(key),
		payload: b.Bytes(),
	}
}

// Get returns redis' result.
func (c *Cmd) Get() (res interface{}, err error) {
	return c.res, c.err
}

// GetString returns the value if it's a single string. If the key is not set
// the returned string will be empty.
func (c *Cmd) GetString() (string, error) {
	if c.err != nil {
		return "", c.err
	}
	if c.res == nil {
		return "", nil
	}
	switch x := c.res.(type) {
	case []byte:
		return string(x), nil
	default:
		return "", fmt.Errorf("unexpected value. have %T, want []byte", x)
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
