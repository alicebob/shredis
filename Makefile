.PHONY: test

test:
	gofmt -w *go && go test
