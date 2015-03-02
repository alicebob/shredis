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

// GetStrings returns the value if it's a string slice. If the key is not set
// the returned slice will be empty.
func (c *Cmd) GetStrings() ([]string, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.res == nil {
		return nil, nil
	}
	s, ok := c.res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected value. have %T, want []interface{}", c.res)
	}
	var res []string
	for _, v := range s {
		switch x := v.(type) {
		case []byte:
			res = append(res, string(x))
		default:
			return nil, fmt.Errorf("unexpected value. have a %T, want [][]byte", x)
		}
	}
	return res, nil
}

// GetInt returns the value of Get() if it's either a int in REPL, or if it's a
// string which can be converterd to an int. If the key is not set the value
// will be 0.
func (c *Cmd) GetInt() (int, error) {
	if c.err != nil {
		return 0, c.err
	}
	if c.res == nil {
		return 0, nil
	}
	switch x := c.res.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case []byte:
		return strconv.Atoi(string(x))
	default:
		return 0, fmt.Errorf("unexpected value. have %T, want int-like", x)
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
