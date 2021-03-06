package shredis

import (
	"bytes"
	"reflect"
	"testing"
)

func TestCommand(t *testing.T) {
	for _, c := range []struct{ have, want *Cmd }{
		{
			have: Build("k", "GET", "k"),
			want: &Cmd{
				hash:    2248277386,
				payload: []byte("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"),
			},
		},
	} {
		if !bytes.Equal(c.have.payload, c.want.payload) {
			t.Errorf("have: %q, want: %q", c.have.payload, c.want.payload)
		}
		if c.have.hash != c.want.hash {
			t.Errorf("have: %v, want: %v", c.have.hash, c.want.hash)
		}
	}
}

func TestGetString(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want string
	}{
		{
			have: &Cmd{
				res: "a string",
			},
			want: "a string",
		},
		{
			have: &Cmd{
				res: 12,
			},
			err:  "unexpected value. have int, want string",
			want: "",
		},
		{
			have: &Cmd{
				res: nil,
			},
			want: "",
		},
	} {
		s, err := c.have.GetString()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}
}

func TestGetStrings(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want []string
	}{
		{
			have: &Cmd{
				res: []interface{}{
					"string one",
					"string two",
				},
			},
			want: []string{"string one", "string two"},
		},
		{
			have: &Cmd{
				res: 12,
			},
			err:  "unexpected value. have int, want []interface{}",
			want: nil,
		},
		{
			have: &Cmd{
				res: nil,
			},
			want: nil,
		},
	} {
		s, err := c.have.GetStrings()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; !reflect.DeepEqual(have, want) {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}
}

func TestGetInt(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want int
	}{
		{
			have: &Cmd{
				res: "42",
			},
			want: 42,
		},
		{
			have: &Cmd{
				res: 12,
			},
			want: 12,
		},
		{
			have: &Cmd{
				res: 12,
			},
			want: 12,
		},
		{
			have: &Cmd{
				res: "a string",
			},
			err: "strconv.ParseInt: parsing \"a string\": invalid syntax",
		},
		// not present is a 0.
		{
			have: &Cmd{
				res: nil,
			},
			want: 0,
		},
	} {
		s, err := c.have.GetInt()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}
}

func TestGetMapStringString(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want map[string]string
	}{
		{
			have: &Cmd{
				res: []interface{}{
					"key one",
					"string one",
					"key two",
					"string two",
				},
			},
			want: map[string]string{
				"key one": "string one",
				"key two": "string two",
			},
		},
		{
			have: &Cmd{
				res: 12,
			},
			err:  "unexpected value. have int, want []interface{}",
			want: nil,
		},
		{
			have: &Cmd{
				res: nil,
			},
			want: nil,
		},
	} {
		s, err := c.have.GetMapStringString()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; !reflect.DeepEqual(have, want) {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}
}

func TestGetMapIntString(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want map[int]string
	}{
		{
			have: &Cmd{
				res: []interface{}{
					"1",
					"string one",
					2,
					"string two",
				},
			},
			want: map[int]string{
				1: "string one",
				2: "string two",
			},
		},
		{
			have: &Cmd{
				res: 12,
			},
			err:  "unexpected value. have int, want []interface{}",
			want: nil,
		},
		{
			have: &Cmd{
				res: nil,
			},
			want: nil,
		},
	} {
		s, err := c.have.GetMapIntString()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; !reflect.DeepEqual(have, want) {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}
}

func TestGetMapStringInt(t *testing.T) {
	for _, c := range []struct {
		have *Cmd
		err  string
		want map[string]int
	}{
		{
			have: &Cmd{
				res: []interface{}{
					"string one",
					"1",
					"string two",
					2,
				},
			},
			want: map[string]int{
				"string one": 1,
				"string two": 2,
			},
		},
		{
			have: &Cmd{
				res: 12,
			},
			err:  "unexpected value. have int, want []interface{}",
			want: nil,
		},
		{
			have: &Cmd{
				res: nil,
			},
			want: nil,
		},
	} {
		s, err := c.have.GetMapStringInt()
		var haveerr string
		if err != nil {
			haveerr = err.Error()
		}
		if have, want := haveerr, c.err; have != want {
			t.Errorf("have: %q, want: %q", have, want)
		}
		if have, want := s, c.want; !reflect.DeepEqual(have, want) {
			t.Errorf("have: %q, want: %q", have, want)
		}
	}

}
func TestCmdErr(t *testing.T) {
	c := Build("test")
	if _, err := c.Get(); err != ErrNotExecuted {
		t.Errorf("have: %q, want: %q", err, ErrNotExecuted)
	}

	c.res = 1
	c.err = nil

	if _, err := c.Get(); err != nil {
		t.Errorf("have: %q, want: nil", err)
	}

	if _, err := c.Get(); err != ErrAlreadyGot {
		t.Errorf("have: %q, want: %q", err, ErrAlreadyGot)
	}
}
