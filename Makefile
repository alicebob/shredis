.PHONY: all gofmt test vet lint install

all: gofmt test vet lint install

test:
	go test

gofmt:
	find . -name \*.go|xargs gofmt -w

vet:
	go vet ./...

lint:
	golint ./...

install:
	go install
