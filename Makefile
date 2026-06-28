.PHONY: build test test-short vet lint clean

build:
	go build ./cmd/fsserver/

test:
	go test ./internal/... ./tests/...

test-short:
	go test -short ./internal/...

vet:
	go vet ./...

lint: vet

clean:
	rm -f fsserver
