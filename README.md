A sharding Redis client, à la twemproxy/nutcracker.

Status: work in progress

See `example_test.go` for usage details.

# Compat

Shredis distributes keys the same as a twemproxy with this config:
```
  hash: fnv1a_64
  distribution: ketama
  auto_eject_hosts: false
```
and all servers having weight 1.

(with the distinction that Shredis won't handle MGET / MSET for you)

# &c.

[![Build Status](https://travis-ci.org/alicebob/shredis.svg?branch=master)](https://travis-ci.org/alicebob/shredis)
