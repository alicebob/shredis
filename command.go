package shredis

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
)

var (
	// ErrNotExecuted is returned by all Get* commands if the Cmd
	// hasn't been executed by Exec().
	ErrNotExecuted = errors.New("command not executed")
	// ErrAlreadyGot is returned if more than one Cmd.Get* is called for a
	// single Exec().
	ErrAlreadyGot = errors.New("result already retrieved")
)

// Cmd is a redis command.
type Cmd struct {
	key     string
	payload []byte
	res     interface{}
	err     error
}

// Build makes a command which will be send to the shard for 'key'. All
// redis commands work, but it's not advised to use commands which are stateful
// ('SELECT'), involve multiple servers ('MGET', 'MGET', 'RENAME'), or are not
// simple command->reply ('WATCH').
func Build(key string, fields ...string) *Cmd {
	return &Cmd{
		key:     key,
		payload: buildCommand(fields, make([]byte, 0, 64)),
		err:     ErrNotExecuted,
	}
}

var poolCmds = sync.Pool{
	New: func() interface{} {
		return &Cmd{}
	},
}

// PooledBuild is same as Build, but uses a sync.Pool. Use Cmd.Put() to put
// them back in the queue.
func PooledBuild(key string, fields ...string) *Cmd {
	c := poolCmds.Get().(*Cmd)
	c.key = key
	c.payload = buildCommand(fields, c.payload[:0])
	c.err = ErrNotExecuted
	return c
}

// Put puts a cmd back in the pool for reuse.
func (c *Cmd) Put() {
	poolCmds.Put(c)
}

// Get returns redis' result.
func (c *Cmd) Get() (interface{}, error) {
	err := c.err
	c.err = ErrAlreadyGot
	return c.res, err
}

// GetString returns the value if it's a single string. If the key is not set
// the returned string will be empty.
func (c *Cmd) GetString() (string, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return "", err
	}
	if c.res == nil {
		return "", nil
	}
	return resString(c.res)
}

// GetStrings returns the value if it's a string slice. If the key is not set
// the returned slice will be empty.
func (c *Cmd) GetStrings() ([]string, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return nil, err
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
		k, err := resString(v)
		if err != nil {
			return nil, err
		}
		res = append(res, k)
	}
	return res, nil
}

// GetInt returns the value of Get() if it's either a int in REPL, or if it's a
// string which can be converterd to an int. If the key is not set the value
// will be 0.
func (c *Cmd) GetInt() (int, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return 0, err
	}
	if c.res == nil {
		return 0, nil
	}
	return resInt(c.res)
}

// GetMapStringString returns the value if it's a map[string]string. If the key
// is not set the returned map will be empty.
func (c *Cmd) GetMapStringString() (map[string]string, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return nil, err
	}
	if c.res == nil {
		return nil, nil
	}
	s, ok := c.res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected value. have %T, want []interface{}", c.res)
	}

	res := make(map[string]string, len(s)/2)
	for len(s) > 1 {
		k, err := resString(s[0])
		if err != nil {
			return nil, err
		}
		v, err := resString(s[1])
		if err != nil {
			return nil, err
		}
		res[k] = v
		s = s[2:]
	}
	return res, nil
}

// GetMapIntString returns the value if it's a map[int]string. If the key
// is not set the returned map will be empty.
func (c *Cmd) GetMapIntString() (map[int]string, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return nil, err
	}
	if c.res == nil {
		return nil, nil
	}
	s, ok := c.res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected value. have %T, want []interface{}", c.res)
	}

	res := make(map[int]string, len(s)/2)
	for len(s) > 1 {
		k, err := resInt(s[0])
		if err != nil {
			return nil, err
		}
		v, err := resString(s[1])
		if err != nil {
			return nil, err
		}

		res[k] = v
		s = s[2:]
	}
	return res, nil
}

// GetMapStringInt returns the value if it's a map[string]int. If the key
// is not set the returned map will be empty.
func (c *Cmd) GetMapStringInt() (map[string]int, error) {
	err := c.err
	c.err = ErrAlreadyGot
	if err != nil {
		return nil, err
	}
	if c.res == nil {
		return nil, nil
	}
	s, ok := c.res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected value. have %T, want []interface{}", c.res)
	}

	res := make(map[string]int, len(s)/2)
	for len(s) > 1 {
		k, err := resString(s[0])
		if err != nil {
			return nil, err
		}
		v, err := resInt(s[1])
		if err != nil {
			return nil, err
		}

		res[k] = v
		s = s[2:]
	}
	return res, nil
}

func resString(x interface{}) (string, error) {
	switch k := x.(type) {
	case string:
		return k, nil
	default:
		return "", fmt.Errorf("unexpected value. have %T, want string", x)
	}
}

func resInt(x interface{}) (int, error) {
	switch k := x.(type) {
	case int:
		return k, nil
	case string:
		return strconv.Atoi(k)
	default:
		return 0, fmt.Errorf("unexpected value. have %T, want int or string", x)
	}
}

func buildCommand(fields []string, b []byte) []byte {
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(fields)), 10)
	b = append(b, '\r', '\n')
	for _, f := range fields {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(f)), 10)
		b = append(b, '\r', '\n')
		b = append(b, f...)
		b = append(b, '\r', '\n')
	}
	return b
}
