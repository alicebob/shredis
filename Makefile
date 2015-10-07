.PHONY: all gofmt test bench prof vet lint install

all: gofmt test vet lint install

test:
	go test

bench:
	go test -run=XXX -bench=. -cpu=1,4 -benchmem

prof:
	go test -run=XXX -bench=. -test.cpuprofile=cpu.out
	go tool pprof shredis.test cpu.out

mem:
	go test -run=XXX -bench=. -test.memprofile=mem.out
	go tool pprof -alloc_objects shredis.test mem.out

gofmt:
	find . -name \*.go|xargs gofmt -w

vet:
	go vet ./...

lint:
	golint ./...

install:
	go install
