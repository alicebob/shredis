package shredis

import (
	"bytes"
	"strconv"
)

type Cmd struct {
	Key     []byte
	Payload []byte
}

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
