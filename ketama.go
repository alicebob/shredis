// Copied and adapted from github.com/dgryski/go-ketama , which has no explicit
// license.

// Package ketama implements consistent hashing compatible with Algorithm::ConsistentHash::Ketama
/*
This implementation draws from the Daisuke Maki's Perl module, which itself is
based on the original libketama code.  That code was licensed under the GPLv2,
and thus so it this.

The major API change from libketama is that Algorithm::ConsistentHash::Ketama allows hashing
arbitrary strings, instead of just memcached server IP addresses.
*/

package shredis

import (
	"crypto/md5"
	"fmt"
	"hash/fnv"
	"sort"
)

type bucket struct {
	Label  string
	Weight int
}

type continuumPoint struct {
	bucket bucket
	point  uint
}

type continuum []continuumPoint

func (c continuum) Less(i, j int) bool { return c[i].point < c[j].point }
func (c continuum) Len() int           { return len(c) }
func (c continuum) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func md5Digest(in string) []byte {
	h := md5.New()
	h.Write([]byte(in))
	return h.Sum(nil)
}

func hashKey(in []byte) uint {
	h := fnv.New64a()
	h.Write(in)
	return uint(uint32(h.Sum64()))
}

func ketamaNew(buckets []bucket) continuum {

	numbuckets := len(buckets)

	if numbuckets == 0 {
		// let them error when they try to use it
		return continuum(nil)
	}

	ket := make([]continuumPoint, 0, numbuckets*160)

	totalweight := 0
	for _, b := range buckets {
		totalweight += b.Weight
	}

	for i, b := range buckets {
		pct := float32(b.Weight) / float32(totalweight)

		// this is the equivalent of C's promotion rules, but in Go, to maintain exact compatibility with the C library
		limit := int(float32(float64(pct) * 40.0 * float64(numbuckets)))

		for k := 0; k < limit; k++ {
			/* 40 hashes, 4 numbers per hash = 160 points per bucket */
			ss := fmt.Sprintf("%s-%d", b.Label, k)
			digest := md5Digest(ss)

			for h := 0; h < 4; h++ {
				point := continuumPoint{
					point:  uint(digest[3+h*4])<<24 | uint(digest[2+h*4])<<16 | uint(digest[1+h*4])<<8 | uint(digest[h*4]),
					bucket: buckets[i],
				}
				ket = append(ket, point)
			}
		}
	}

	cont := continuum(ket)

	sort.Sort(cont)

	return cont
}

func (c continuum) Hash(thing []byte) string {
	if len(c) == 0 {
		return ""
	}

	h := hashKey(thing)
	i := sort.Search(len(c), func(i int) bool { return c[i].point >= h })
	if i >= len(c) {
		i = 0
	}
	return c[i].bucket.Label
}
