package shredis

import (
	"testing"
)

func TestParseInt(t *testing.T) {
	for _, tc := range []struct {
		s    string
		want int64
		err  string
	}{
		{
			s:    "12",
			want: 12,
		},
		{
			s:    "-12",
			want: -12,
		},
		{
			s:    "-0",
			want: 0,
		},
		{
			s:   "foo",
			err: "shredis: illegal bytes in length",
		},
		{
			s:   "-foo",
			err: "shredis: illegal bytes in length",
		},
	} {
		have, haveErr := parseInt([]byte(tc.s))
		if tc.err == "" {
			if haveErr != nil {
				t.Errorf("have %v, want nil", haveErr)
				continue
			}
		} else {
			if haveErr == nil || haveErr.Error() != tc.err {
				t.Errorf("have %v, want %v", haveErr, tc.err)
				continue
			}
		}
		if have != tc.want {
			t.Errorf("have %v, want %v", have, tc.want)
		}
	}
}
