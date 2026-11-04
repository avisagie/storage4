.PHONY: check fmt vet staticcheck test build

check: fmt vet staticcheck test

fmt:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

test:
	go test -race ./...

build:
	CGO_ENABLED=0 go build -o bin/store ./cmd/store
