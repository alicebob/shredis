#!/bin/sh
# run this and in another terminal: ./generate.sh
# if this script prints nothing all is fine.
go build planb.go
~/src/nutcracker-0.4.0/src/nutcracker -c planb.conf -v 8 2>&1 | \
    grep 'maps to server' | \
    awk '{print $5, $12}' | \
    ./planb
